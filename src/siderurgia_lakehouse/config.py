"""Configuração central: parâmetros de cada fonte e de execução.

Tudo que muda entre ambientes (local x Databricks, dev x prod) vem de variáveis de
ambiente ou argumentos de linha de comando — nunca fica fixo no código.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

# ----------------------------------------------------------------------------- IBGE / SIDRA
# Tabela 8888 — Produção Física Industrial por seções e atividades (PIM-PF, base 2022=100).
# Substituiu a tabela 8159 em mar/2023.
SIDRA_TABELA = 8888
SIDRA_CLASSIFICACAO = 544  # Seções e atividades industriais (CNAE 2.0)
SIDRA_VARIAVEIS = {
    12606: "indice",  # Número-índice (2022=100)
    12607: "indice_sa",  # Número-índice com ajuste sazonal (2022=100)
}
# As atividades são identificadas pelo nome na silver/gold (mais robusto que códigos
# internos de categoria, que o IBGE pode renumerar entre versões da tabela).
ATIVIDADE_METALURGIA = "metalurgia"
ATIVIDADE_TRANSFORMACAO = "transformação"
ATIVIDADE_INDUSTRIA_GERAL = "indústria geral"

# ----------------------------------------------------------------------------- Comex Stat (MDIC)
# Capítulo 72 do Sistema Harmonizado = "Ferro fundido, ferro e aço".
COMEX_CAPITULOS = ["72"]

# ----------------------------------------------------------------------------- Climate TRACE
CLIMATE_TRACE_PAIS = "BRA"
CLIMATE_TRACE_SUBSETOR = "iron-and-steel"
CLIMATE_TRACE_GAS = "co2e_100yr"
CLIMATE_TRACE_PRIMEIRO_ANO = 2021  # a API v7 não tem dados por usina antes disso

# ----------------------------------------------------------------------------- Banco Central
BCB_SERIE_DOLAR = 1  # Dólar americano (venda) - PTAX, R$/US$, diária


def _em_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _data_env(nome: str, padrao: str) -> date:
    return date.fromisoformat(os.getenv(nome, padrao))


@dataclass
class Settings:
    """Parâmetros de execução do pipeline."""

    ambiente: str = field(
        default_factory=lambda: os.getenv(
            "LAKEHOUSE_ENV", "databricks" if _em_databricks() else "local"
        )
    )
    # Local: tabelas Delta gravadas por caminho dentro desta pasta.
    base_path: str = field(
        default_factory=lambda: os.getenv("LAKEHOUSE_BASE_PATH", str(Path("lakehouse").resolve()))
    )
    # Databricks: tabelas gerenciadas no Unity Catalog → <catalog>.<camada>.<tabela>
    catalog: str = field(
        default_factory=lambda: os.getenv("LAKEHOUSE_CATALOG", "siderurgia_lakehouse")
    )
    # Início do histórico na primeira carga.
    data_inicio: date = field(default_factory=lambda: _data_env("DATA_INICIO", "2015-01-01"))
    # Cargas incrementais reprocessam uma janela recente, porque as fontes revisam
    # dados já publicados (IBGE revisa a PIM-PF; o MDIC consolida meses recentes).
    sidra_ultimos_meses: int = field(
        default_factory=lambda: int(os.getenv("SIDRA_ULTIMOS_MESES", "24"))
    )
    comex_anos_reprocessar: int = field(
        default_factory=lambda: int(os.getenv("COMEX_ANOS_REPROCESSAR", "2"))
    )
    # Anos pedidos por consulta ao Comex Stat (~5 mil linhas/ano; limite ~150 mil).
    comex_anos_por_consulta: int = field(
        default_factory=lambda: int(os.getenv("COMEX_ANOS_POR_CONSULTA", "4"))
    )
    climate_trace_anos_reprocessar: int = field(
        default_factory=lambda: int(os.getenv("CLIMATE_TRACE_ANOS_REPROCESSAR", "3"))
    )
    lookback_dias: int = field(default_factory=lambda: int(os.getenv("LOOKBACK_DIAS", "45")))

    @property
    def landing_path(self) -> str:
        """Zona de pouso dos JSONs brutos (arquivos, antes de virar tabela)."""
        if self.ambiente == "databricks":
            return f"/Volumes/{self.catalog}/bronze/landing"
        return f"{self.base_path}/landing"
