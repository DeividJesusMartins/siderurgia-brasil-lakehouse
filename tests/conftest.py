from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from siderurgia_lakehouse.transform.bronze import montar_bronze


@pytest.fixture(scope="session")
def spark():
    """Spark local *sem* Delta: os testes de transformação são puros (DataFrame → DataFrame),
    então rodam rápido e não dependem de baixar JARs do Maven."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("siderurgia-lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def bronze(spark):
    """Fábrica de DataFrames bronze a partir de registros como a API devolveria."""

    def _fabrica(fonte, registros, requisicao=None, lote="L1", ingerido=datetime(2024, 6, 1)):
        return montar_bronze(
            spark,
            registros,
            fonte=fonte,
            url="teste",
            requisicao=requisicao or {},
            arquivo_landing=None,
            id_lote=lote,
            ingerido_em=ingerido,
        )

    return _fabrica


def sidra(mes: str, valor: str, atividade=("129334", "3.24 Metalurgia"), variavel="12606"):
    """Registro no formato da API SIDRA (dimensões D1..D4)."""
    return {
        "NC": "1", "NN": "Brasil", "MC": "30", "MN": "Número-índice", "V": valor,
        "D1C": "1", "D1N": "Brasil",
        "D2C": variavel, "D2N": "PIMPF - Número-índice (2022=100)",
        "D3C": mes, "D3N": mes,
        "D4C": atividade[0], "D4N": atividade[1],
    }  # fmt: skip


def comex(ano, mes, pais, fob, kg, sh4="7208", desc="Laminados planos de ferro ou aço"):
    """Registro no formato da API Comex Stat (detalhamento country + heading)."""
    return {
        "year": str(ano), "monthNumber": str(mes), "country": pais,
        "headingCode": sh4, "heading": desc, "metricFOB": str(fob), "metricKG": str(kg),
    }  # fmt: skip


def usina(uid, nome, emissoes, ano=2023, pais="BRA", subsetor="iron-and-steel"):
    """Registro no formato da API Climate TRACE v7 (/sources)."""
    return {
        "id": uid, "name": nome, "sector": "manufacturing", "subsector": subsetor,
        "country": pais, "centroid": {"latitude": -20.1, "longitude": -40.2},
        "gas": "co2e_100yr", "emissionsQuantity": emissoes, "year": ano,
    }  # fmt: skip
