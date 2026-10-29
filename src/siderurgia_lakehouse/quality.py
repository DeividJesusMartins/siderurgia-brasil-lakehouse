"""Checagens de qualidade de dados (data quality) executadas entre as camadas.

Implementação própria e enxuta para deixar o conceito visível. Em produção no
Databricks, o equivalente nativo são as *expectations* do Lakeflow Declarative
Pipelines (antigo Delta Live Tables) ou bibliotecas como Great Expectations / DQX.

Cada checagem conta as linhas que violam a regra. Severidade "erro" interrompe o
pipeline (a camada seguinte não é gravada); "alerta" só registra no log.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Resultado:
    nome: str
    severidade: str
    violacoes: int

    @property
    def passou(self) -> bool:
        return self.violacoes == 0


Check = Callable[[DataFrame], Resultado]


class DataQualityError(RuntimeError):
    pass


def nao_nulo(*colunas: str, severidade: str = "erro") -> Check:
    def _check(df: DataFrame) -> Resultado:
        cond = F.lit(False)
        for c in colunas:
            cond = cond | F.col(c).isNull()
        return Resultado(f"nao_nulo{colunas}", severidade, df.filter(cond).count())

    return _check


def unico(*chaves: str, severidade: str = "erro") -> Check:
    def _check(df: DataFrame) -> Resultado:
        dup = df.groupBy(*chaves).count().filter(F.col("count") > 1).count()
        return Resultado(f"unico{chaves}", severidade, dup)

    return _check


def entre(
    coluna: str, minimo: float, maximo: float, filtro: str | None = None, severidade="alerta"
) -> Check:
    def _check(df: DataFrame) -> Resultado:
        alvo = df.filter(filtro) if filtro else df
        fora = alvo.filter(~F.col(coluna).between(minimo, maximo)).count()
        nome = f"entre({coluna}, {minimo}, {maximo}{', ' + filtro if filtro else ''})"
        return Resultado(nome, severidade, fora)

    return _check


def nao_vazio(severidade: str = "erro") -> Check:
    def _check(df: DataFrame) -> Resultado:
        return Resultado("nao_vazio", severidade, 0 if df.limit(1).count() else 1)

    return _check


def executar(df: DataFrame, checks: Sequence[Check], tabela: str) -> list[Resultado]:
    resultados = [check(df) for check in checks]
    for r in resultados:
        nivel = logging.INFO if r.passou else logging.WARNING
        logger.log(nivel, "[DQ %s] %s → %d violações (%s)", tabela, r.nome, r.violacoes,
                   r.severidade)  # fmt: skip

    falhas = [r for r in resultados if not r.passou and r.severidade == "erro"]
    if falhas:
        detalhes = "; ".join(f"{r.nome}={r.violacoes}" for r in falhas)
        raise DataQualityError(f"{tabela} reprovada na qualidade de dados: {detalhes}")
    return resultados
