"""Orquestração das camadas. Cada função é uma etapa independente (uma task no Job).

O comportamento de cada fonte na silver é declarado em `SPECS` (tabela de destino,
chaves, estratégia de escrita). O código de orquestração é o mesmo para todas.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from siderurgia_lakehouse import quality as dq
from siderurgia_lakehouse.config import Settings
from siderurgia_lakehouse.ingestion.fontes import Fonte
from siderurgia_lakehouse.ingestion.landing import salvar_lote
from siderurgia_lakehouse.lakehouse import Lakehouse
from siderurgia_lakehouse.transform import gold, silver
from siderurgia_lakehouse.transform.bronze import montar_bronze

logger = logging.getLogger(__name__)

QUARENTENA = ("silver", "quarentena")


@dataclass(frozen=True)
class SpecSilver:
    fonte: str  # nome da fonte = tabela na bronze
    tabela: str  # tabela na silver
    construir: Callable[[DataFrame, date], tuple[DataFrame, DataFrame]]
    chaves: tuple[str, ...]
    colunas_valor: tuple[str, ...]  # se nenhuma mudar, o MERGE não reescreve a linha
    coluna_periodo: str  # base da marca d'água e do intervalo substituído
    estrategia: str  # "merge" ou "substituir"


SPECS: dict[str, SpecSilver] = {
    s.fonte: s
    for s in [
        SpecSilver(
            "sidra_pimpf", "producao_industrial",
            lambda df, hoje: silver.construir_producao(df),
            silver.CHAVES_PRODUCAO, ("valor", "atividade_nome"), "mes_referencia", "merge",
        ),
        # O Comex Stat devolve cada ano inteiro e consolidado: substituir o período
        # (replaceWhere) também remove linhas que a fonte deixou de informar.
        SpecSilver(
            "comexstat_exportacoes", "exportacoes_aco",
            lambda df, hoje: silver.construir_exportacoes(df),
            silver.CHAVES_EXPORTACAO, ("valor_fob_usd", "peso_kg"), "mes_referencia",
            "substituir",
        ),
        SpecSilver(
            "climatetrace_usinas", "emissoes_usinas",
            lambda df, hoje: silver.construir_emissoes(df),
            silver.CHAVES_EMISSOES, ("emissoes_t", "usina_nome", "latitude", "longitude"),
            "ano", "merge",
        ),
        SpecSilver(
            "bcb_cambio", "cambio_dolar",
            silver.construir_cambio,
            silver.CHAVES_CAMBIO, ("dolar_ptax",), "data_referencia", "merge",
        ),
    ]
}  # fmt: skip


class PipelineError(RuntimeError):
    pass


def marca_dagua(lh: Lakehouse, spec: SpecSilver) -> date | None:
    """Último período já carregado na silver (high watermark)."""
    if not lh.existe("silver", spec.tabela):
        return None
    valor = lh.ler("silver", spec.tabela).agg(F.max(spec.coluna_periodo)).first()[0]
    if isinstance(valor, int):  # período anual (Climate TRACE)
        return date(valor, 12, 31)
    return valor


def _falhar_se_houver(erros: dict[str, Exception], etapa: str) -> None:
    if erros:
        resumo = "; ".join(f"{nome}: {exc}" for nome, exc in erros.items())
        raise PipelineError(f"{etapa} falhou para {len(erros)} fonte(s) — {resumo}")


# --------------------------------------------------------------------------- bronze
def executar_bronze(
    spark: SparkSession,
    lh: Lakehouse,
    settings: Settings,
    fontes: Sequence[Fonte],
    full_refresh: bool = False,
    hoje: date | None = None,
) -> dict[str, int]:
    """Ingere cada fonte de forma isolada: se uma API cair, as outras seguem e o erro
    é reportado no fim (a task falha, mas sem perder o que deu certo)."""
    hoje = hoje or date.today()
    agora = datetime.now()
    id_lote = agora.strftime("%Y%m%d%H%M%S")
    totais: dict[str, int] = {}
    erros: dict[str, Exception] = {}

    for fonte in fontes:
        try:
            marca = None if full_refresh else marca_dagua(lh, SPECS[fonte.nome])
            logger.info("[%s] marca d'água: %s", fonte.nome, marca or "nenhuma (carga completa)")
            frames: list[DataFrame] = []
            for lote in fonte.extrair(marca, hoje):
                arquivo = salvar_lote(
                    settings.landing_path, fonte.nome, id_lote, lote.descricao, lote.registros
                )
                if lote.registros:
                    frames.append(
                        montar_bronze(
                            spark,
                            lote.registros,
                            fonte=fonte.nome,
                            url=fonte.url,
                            requisicao=lote.requisicao,
                            arquivo_landing=arquivo,
                            id_lote=id_lote,
                            ingerido_em=agora,
                        )
                    )
            if not frames:
                logger.warning("[%s] nenhum registro retornado", fonte.nome)
                totais[fonte.nome] = 0
                continue
            # Uma escrita por fonte (e não por requisição): menos commits no log do
            # Delta e menos arquivos pequenos — o famoso "small files problem".
            bronze = reduce(DataFrame.unionByName, frames)
            totais[fonte.nome] = bronze.count()
            lh.anexar(bronze, "bronze", fonte.nome)
            logger.info("[%s] bronze: %d registros no lote %s", fonte.nome,
                        totais[fonte.nome], id_lote)  # fmt: skip
        except Exception as exc:
            logger.exception("[%s] falha na ingestão", fonte.nome)
            erros[fonte.nome] = exc

    _falhar_se_houver(erros, "Bronze")
    return totais


# --------------------------------------------------------------------------- silver
def _processar_silver(lh: Lakehouse, spec: SpecSilver, full_refresh: bool, hoje: date) -> int:
    if not lh.existe("bronze", spec.fonte):
        logger.warning("[%s] bronze inexistente — rode a ingestão antes", spec.fonte)
        return 0

    bronze = lh.ler("bronze", spec.fonte)
    incremental = not full_refresh and lh.existe("silver", spec.tabela)
    if incremental:
        # Só o que chegou na bronze depois da última carga desta tabela.
        corte = lh.ler("silver", spec.tabela).agg(F.max("_ingerido_em")).first()[0]
        if corte is not None:
            bronze = bronze.filter(F.col("_ingerido_em") > F.lit(corte))

    df, quarentena = spec.construir(bronze, hoje)
    # cache: o DataFrame alimenta várias ações (contagem, checks de DQ, escrita).
    # Sem cache, o Spark releria e reprocessaria a bronze em cada uma delas.
    df = df.cache()
    try:
        total = df.count()
        if total == 0:
            logger.info("[%s] nada novo para a silver", spec.fonte)
        else:
            dq.executar(
                df,
                [dq.nao_nulo(*spec.chaves), dq.unico(*spec.chaves)],
                f"silver.{spec.tabela}",
            )
            if not incremental:
                lh.sobrescrever(df, "silver", spec.tabela)
            elif spec.estrategia == "substituir":
                inicio, fim = df.agg(F.min(spec.coluna_periodo), F.max(spec.coluna_periodo)).first()
                lh.substituir_intervalo(df, "silver", spec.tabela, spec.coluna_periodo,
                                        inicio, fim)  # fmt: skip
            else:
                lh.upsert(df, "silver", spec.tabela, spec.chaves, spec.colunas_valor)
    finally:
        df.unpersist()

    rejeitadas = quarentena.count()
    if rejeitadas:
        logger.warning("[%s] %d registros enviados para a quarentena", spec.fonte, rejeitadas)
        lh.anexar(quarentena, *QUARENTENA)
    logger.info("[%s] silver.%s: %d registros processados", spec.fonte, spec.tabela, total)
    return total


def executar_silver(
    spark: SparkSession,
    lh: Lakehouse,
    fontes: Sequence[str] | None = None,
    full_refresh: bool = False,
    hoje: date | None = None,
) -> dict[str, int]:
    hoje = hoje or date.today()
    totais, erros = {}, {}
    for nome in fontes or SPECS:
        try:
            totais[nome] = _processar_silver(lh, SPECS[nome], full_refresh, hoje)
        except Exception as exc:
            logger.exception("[%s] falha na silver", nome)
            erros[nome] = exc
    _falhar_se_houver(erros, "Silver")
    return totais


# --------------------------------------------------------------------------- gold
def executar_gold(spark: SparkSession, lh: Lakehouse) -> dict[str, int]:
    """Recalcula a gold inteira a partir da silver.

    Recalcular tudo é uma escolha consciente: o volume é pequeno (milhares de linhas)
    e as métricas de 12 meses dependem do histórico — um MERGE incremental seria mais
    complexo sem ganho real.
    """
    obrigatorias = ["producao_industrial", "exportacoes_aco", "cambio_dolar"]
    faltando = [t for t in obrigatorias if not lh.existe("silver", t)]
    if faltando:
        raise PipelineError(f"Silver incompleta, faltam: {faltando}")

    producao = lh.ler("silver", "producao_industrial")
    exportacoes = lh.ler("silver", "exportacoes_aco")
    cambio = lh.ler("silver", "cambio_dolar")

    dim_atividade = gold.construir_dim_atividade(producao).cache()
    fato_producao = gold.construir_fato_producao(producao).cache()
    fato_exportacao = gold.construir_fato_exportacao(exportacoes, cambio).cache()
    painel = gold.construir_painel_mensal(fato_producao, dim_atividade, fato_exportacao)
    painel = painel.cache()

    inicio, fim = painel.agg(F.min("mes_referencia"), F.max("mes_referencia")).first()
    tabelas: dict[str, DataFrame] = {
        "dim_atividade": dim_atividade,
        "dim_calendario": gold.construir_dim_calendario(spark, inicio, fim),
        "fato_producao_industrial": fato_producao,
        "fato_exportacao_aco": fato_exportacao,
        "painel_siderurgia_mensal": painel,
    }

    dq.executar(fato_producao, [dq.nao_vazio(), dq.unico("atividade_codigo", "mes_referencia")],
                "gold.fato_producao_industrial")  # fmt: skip
    dq.executar(
        fato_exportacao,
        [
            dq.unico("mes_referencia", "pais", "sh4_codigo"),
            # Faixas de sanidade: fora delas, provavelmente erro de unidade ou parsing.
            dq.entre("preco_medio_usd_t", 50, 50_000, filtro="peso_t > 1"),
        ],
        "gold.fato_exportacao_aco",
    )
    dq.executar(
        painel,
        [
            dq.nao_vazio(),
            dq.unico("mes_referencia"),
            dq.entre("dolar_medio", 0.5, 20, filtro="dolar_medio IS NOT NULL"),
            dq.entre("indice_metalurgia", 1, 500, filtro="indice_metalurgia IS NOT NULL"),
            dq.nao_nulo("indice_metalurgia", severidade="alerta"),
        ],
        "gold.painel_siderurgia_mensal",
    )

    # Emissões são opcionais: a API do Climate TRACE é beta. Sem ela, o resto da
    # gold é publicado normalmente e o log avisa.
    if lh.existe("silver", "emissoes_usinas"):
        fato_emissoes = gold.construir_fato_emissoes(lh.ler("silver", "emissoes_usinas")).cache()
        dq.executar(fato_emissoes, [dq.unico("usina_id", "ano"), dq.nao_vazio("alerta")],
                    "gold.fato_emissoes_usina")  # fmt: skip
        tabelas["fato_emissoes_usina"] = fato_emissoes
        tabelas["painel_emissoes_anual"] = gold.construir_painel_emissoes(fato_emissoes)
    else:
        logger.warning("silver.emissoes_usinas inexistente: tabelas de emissões não publicadas")

    totais = {}
    try:
        for nome, df in tabelas.items():
            lh.sobrescrever(df, "gold", nome)
            totais[nome] = df.count()
    finally:
        for df in tabelas.values():
            df.unpersist()
    logger.info("Gold publicada: %s", totais)
    return totais


# --------------------------------------------------------------------------- manutenção
def executar_manutencao(spark: SparkSession, lh: Lakehouse, retencao_horas: int = 168) -> None:
    """OPTIMIZE compacta arquivos pequenos; ZORDER co-localiza dados pelas colunas mais
    filtradas (data skipping); VACUUM apaga arquivos antigos fora da retenção — o que
    também limita até onde o Time Travel consegue voltar.

    No Databricks, com Predictive Optimization ligado no Unity Catalog, isso é automático.
    """
    alvos = [("silver", s.tabela, ", ".join(s.chaves[:2])) for s in SPECS.values()]
    alvos += [("bronze", s.fonte, None) for s in SPECS.values()]
    for camada, tabela, zorder in alvos:
        if not lh.existe(camada, tabela):
            continue
        nome = lh.nome(camada, tabela)
        spark.sql(f"OPTIMIZE {nome}" + (f" ZORDER BY ({zorder})" if zorder else ""))
        spark.sql(f"VACUUM {nome} RETAIN {retencao_horas} HOURS")
        logger.info("Manutenção concluída em %s.%s", camada, tabela)
