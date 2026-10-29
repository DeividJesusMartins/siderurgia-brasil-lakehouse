"""Fontes de dados. Cada uma sabe *como perguntar* à sua API e devolve lotes de
registros brutos — sem nenhuma transformação de negócio (isso é papel da silver).

Todas seguem o mesmo contrato (`Fonte`), então o pipeline trata as quatro igual:

    for lote in fonte.extrair(marca_dagua, hoje):
        salvar na landing → anexar na bronze

`marca_dagua` é a última data já carregada na silver (None = carga completa). Cada fonte
decide quanto voltar no tempo a partir dela, porque cada uma revisa dados de um jeito.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Protocol
from urllib.parse import quote

from siderurgia_lakehouse import config
from siderurgia_lakehouse.config import Settings
from siderurgia_lakehouse.ingestion.http import ApiError, HttpClient
from siderurgia_lakehouse.ingestion.janelas import gerar_janelas

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Lote:
    """Resultado de uma requisição: vira um arquivo na landing e N linhas na bronze."""

    descricao: str  # nome do arquivo na landing (ex.: "2024", "20200101_20241231")
    requisicao: dict[str, Any]  # parâmetros usados — guardados para linhagem/auditoria
    registros: list[dict[str, Any]]


class Fonte(Protocol):
    nome: str  # também é o nome da tabela na bronze
    url: str

    def extrair(self, marca_dagua: date | None, hoje: date) -> Iterator[Lote]: ...


# ============================================================================ IBGE / SIDRA
class SidraPimPf:
    """Produção Física Industrial (IBGE, tabela 8888) — índices mensais por atividade.

    Formato da API: https://apisidra.ibge.gov.br/values/t/{tabela}/n1/all/v/{vars}/p/{periodo}/c{cls}/all
    A resposta é uma lista em que o 1º elemento é o *cabeçalho* (rótulos das colunas) e
    os demais são dados com chaves V (valor), D1C/D1N (Brasil), D2C/D2N (variável),
    D3C/D3N (mês, AAAAMM), D4C/D4N (atividade).
    """

    nome = "sidra_pimpf"
    url = "https://apisidra.ibge.gov.br/values"

    def __init__(self, settings: Settings, http: HttpClient | None = None) -> None:
        self.settings = settings
        self.http = http or HttpClient()

    def extrair(self, marca_dagua: date | None, hoje: date) -> Iterator[Lote]:
        # Volume pequeno (~30 atividades x 12 meses/ano): a carga completa é uma requisição
        # só. Na incremental, pedimos os últimos N meses para absorver revisões do IBGE.
        periodo = "all" if marca_dagua is None else f"last {self.settings.sidra_ultimos_meses}"
        variaveis = ",".join(str(v) for v in config.SIDRA_VARIAVEIS)
        caminho = (
            f"/t/{config.SIDRA_TABELA}/n1/all/v/{variaveis}/p/{periodo}"
            f"/c{config.SIDRA_CLASSIFICACAO}/all"
        )
        resposta = self.http.get(self.url + quote(caminho, safe="/,"))
        if not isinstance(resposta, list):
            raise ApiError(f"SIDRA: resposta inesperada: {str(resposta)[:200]}")

        # Remove a linha de cabeçalho (a que tem o rótulo "Valor" no campo V).
        registros = [r for r in resposta if r.get("V") != "Valor"]
        logger.info("SIDRA t/%s p/%s → %d registros", config.SIDRA_TABELA, periodo, len(registros))
        yield Lote(
            descricao=periodo.replace(" ", "_"),
            requisicao={"caminho": caminho},
            registros=registros,
        )


# ============================================================================ Comex Stat
class ComexStatExportacoes:
    """Exportações brasileiras de ferro e aço (capítulo 72 do SH) por país e posição SH4.

    POST https://api-comexstat.mdic.gov.br/general  (documentação: /docs)

    A API tem dois limites: ~150 mil linhas por consulta e poucas consultas por
    minuto (HTTP 429). Cada ano do capítulo 72 tem ~5 mil linhas, então pedimos
    vários anos por consulta (padrão: 4) — a carga completa vira 3 requisições
    em vez de 12, bem longe dos dois limites.
    """

    nome = "comexstat_exportacoes"
    url = "https://api-comexstat.mdic.gov.br/general"

    def __init__(self, settings: Settings, http: HttpClient | None = None) -> None:
        self.settings = settings
        # A API limita a frequência de chamadas e, ao estourar, pede ~10 s de espera.
        # Espaçamos as requisições para nem chegar ao limite e toleramos mais tentativas.
        self.http = http or HttpClient(timeout=120, max_tentativas=8, pausa_entre_requisicoes=12)

    def anos(self, marca_dagua: date | None, hoje: date) -> range:
        primeiro = self.settings.data_inicio.year
        if marca_dagua is not None:
            # O MDIC consolida os números dos meses recentes: reprocessamos os últimos
            # N anos inteiros (ou desde a marca d'água, se ela for mais antiga).
            recente = hoje.year - self.settings.comex_anos_reprocessar + 1
            primeiro = max(primeiro, min(marca_dagua.year, recente))
        return range(primeiro, hoje.year + 1)

    def blocos(self, marca_dagua: date | None, hoje: date) -> list[range]:
        """Agrupa os anos em blocos consecutivos de até N anos (uma consulta por bloco)."""
        anos = list(self.anos(marca_dagua, hoje))
        n = max(1, self.settings.comex_anos_por_consulta)
        return [range(anos[i], anos[min(i + n, len(anos)) - 1] + 1) for i in range(0, len(anos), n)]

    def extrair(self, marca_dagua: date | None, hoje: date) -> Iterator[Lote]:
        for bloco in self.blocos(marca_dagua, hoje):
            inicio, fim = bloco[0], bloco[-1]
            ultimo_mes = hoje.month if fim == hoje.year else 12
            corpo = {
                "flow": "export",
                "monthDetail": True,
                "period": {"from": f"{inicio}-01", "to": f"{fim}-{ultimo_mes:02d}"},
                "filters": [{"filter": "chapter", "values": config.COMEX_CAPITULOS}],
                "details": ["country", "heading"],
                "metrics": ["metricFOB", "metricKG"],
            }
            resposta = self.http.post(self.url, json=corpo, params={"language": "pt"})
            registros = self._extrair_lista(resposta)
            descricao = str(inicio) if inicio == fim else f"{inicio}_{fim}"
            logger.info("Comex Stat %s → %d registros", descricao, len(registros))
            yield Lote(descricao=descricao, requisicao=corpo, registros=registros)

    @staticmethod
    def _extrair_lista(resposta: Any) -> list[dict]:
        """A resposta vem como {"data": {"list": [...]}, ...}; aceitamos também a lista
        direto em "data" para não quebrar se a API simplificar o envelope."""
        if resposta is None:
            return []
        dados = resposta.get("data") if isinstance(resposta, dict) else resposta
        if isinstance(dados, dict):
            dados = dados.get("list")
        if not isinstance(dados, list):
            raise ApiError(f"Comex Stat: formato de resposta inesperado: {str(resposta)[:200]}")
        return dados


# ============================================================================ Climate TRACE
class ClimateTraceUsinas:
    """Emissões estimadas por usina siderúrgica no Brasil (Climate TRACE, API v7 — beta).

    GET https://api.climatetrace.org/v7/sources  — paginada por limit/offset.
    Os dados são anuais e saem com defasagem, então pedimos até o ano anterior.
    A API pede uso moderado: poucas requisições, sequenciais e com limite de páginas.
    """

    nome = "climatetrace_usinas"
    url = "https://api.climatetrace.org/v7/sources"
    por_pagina = 100
    max_paginas = 20  # trava de segurança contra paginação infinita

    def __init__(self, settings: Settings, http: HttpClient | None = None) -> None:
        self.settings = settings
        self.http = http or HttpClient(status_vazio=(404,), pausa_entre_requisicoes=1)

    def anos(self, marca_dagua: date | None, hoje: date) -> range:
        ultimo = hoje.year - 1
        primeiro = max(self.settings.data_inicio.year, config.CLIMATE_TRACE_PRIMEIRO_ANO)
        if marca_dagua is not None:
            primeiro = max(primeiro, ultimo - self.settings.climate_trace_anos_reprocessar + 1)
        return range(primeiro, ultimo + 1)

    def extrair(self, marca_dagua: date | None, hoje: date) -> Iterator[Lote]:
        for ano in self.anos(marca_dagua, hoje):
            base = {
                "year": ano,
                "gas": config.CLIMATE_TRACE_GAS,
                "subsectors": config.CLIMATE_TRACE_SUBSETOR,
                # O filtro de país desta API se chama gadmId (código ISO3, ex.: BRA).
                # Nomes como "countries" são ignorados em silêncio → viria o mundo todo.
                "gadmId": config.CLIMATE_TRACE_PAIS,
                "limit": self.por_pagina,
            }
            registros: list[dict] = []
            for pagina in range(self.max_paginas):
                itens = self._extrair_lista(
                    self.http.get(self.url, params={**base, "offset": pagina * self.por_pagina})
                )
                registros.extend(itens)
                if len(itens) < self.por_pagina:
                    break
            else:
                logger.warning("Climate TRACE %s: limite de %d páginas atingido", ano,
                               self.max_paginas)  # fmt: skip
            logger.info("Climate TRACE %s → %d registros", ano, len(registros))
            yield Lote(descricao=str(ano), requisicao=base, registros=registros)

    @staticmethod
    def _extrair_lista(resposta: Any) -> list[dict]:
        if resposta is None:
            return []
        if isinstance(resposta, list):
            return resposta
        for chave in ("sources", "data", "items", "results"):
            if isinstance(resposta.get(chave), list):
                return resposta[chave]
        raise ApiError(f"Climate TRACE: formato de resposta inesperado: {str(resposta)[:200]}")


# ============================================================================ Banco Central
class BcbCambio:
    """Dólar PTAX venda (SGS série 1), usado para converter exportações para reais.

    GET https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados?formato=json&dataInicial=..&dataFinal=..
    Séries diárias recusam janelas > 10 anos (HTTP 406): consultamos de 5 em 5 anos.
    """

    nome = "bcb_cambio"
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{config.BCB_SERIE_DOLAR}/dados"

    def __init__(self, settings: Settings, http: HttpClient | None = None) -> None:
        self.settings = settings
        self.http = http or HttpClient(status_vazio=(404,))

    def extrair(self, marca_dagua: date | None, hoje: date) -> Iterator[Lote]:
        inicio = self.settings.data_inicio
        if marca_dagua is not None:
            inicio = max(inicio, marca_dagua - timedelta(days=self.settings.lookback_dias))

        for janela in gerar_janelas(inicio, hoje, anos=5):
            params = {
                "formato": "json",
                "dataInicial": janela.inicio.strftime("%d/%m/%Y"),
                "dataFinal": janela.fim.strftime("%d/%m/%Y"),
            }
            registros = self.http.get(self.url, params=params) or []
            logger.info("BCB câmbio %s a %s → %d registros", janela.inicio, janela.fim,
                        len(registros))  # fmt: skip
            yield Lote(
                descricao=f"{janela.inicio:%Y%m%d}_{janela.fim:%Y%m%d}",
                requisicao=params,
                registros=registros,
            )


def todas_as_fontes(settings: Settings) -> list[Fonte]:
    return [
        SidraPimPf(settings),
        ComexStatExportacoes(settings),
        ClimateTraceUsinas(settings),
        BcbCambio(settings),
    ]
