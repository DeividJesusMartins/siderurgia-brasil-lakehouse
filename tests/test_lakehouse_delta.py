"""Testes de integração com Delta Lake real (MERGE, histórico de versões).

Rodam numa invocação separada do pytest (`make test-delta`), porque precisam de uma
SparkSession com os JARs do Delta — baixados do Maven Central na primeira execução.
"""

from __future__ import annotations

from datetime import date

import pytest

from siderurgia_lakehouse.config import Settings
from siderurgia_lakehouse.lakehouse import Lakehouse

pytestmark = pytest.mark.delta


@pytest.fixture(scope="module")
def spark_delta():
    from siderurgia_lakehouse.spark import get_spark

    session = get_spark("siderurgia-lakehouse-delta-tests")
    yield session
    session.stop()


@pytest.fixture
def lh(spark_delta, tmp_path):
    return Lakehouse(spark_delta, Settings(ambiente="local", base_path=str(tmp_path)))


def _df(spark, linhas):
    return spark.createDataFrame(
        linhas, "serie_codigo INT, data_referencia DATE, valor DOUBLE, _criado_em STRING"
    )


def test_upsert_insere_atualiza_e_e_idempotente(spark_delta, lh):
    chaves, valores = ["serie_codigo", "data_referencia"], ["valor"]
    lh.upsert(_df(spark_delta, [(1, date(2024, 1, 1), 5.0, "v1")]), "silver", "t", chaves, valores)
    assert lh.existe("silver", "t")

    novo = _df(spark_delta, [(1, date(2024, 1, 1), 5.1, "v2"), (1, date(2024, 1, 2), 5.2, "v2")])
    lh.upsert(novo, "silver", "t", chaves, valores)
    lh.upsert(novo, "silver", "t", chaves, valores)  # mesma carga de novo

    linhas = {r.data_referencia: r for r in lh.ler("silver", "t").collect()}
    assert len(linhas) == 2
    assert linhas[date(2024, 1, 1)].valor == 5.1
    assert linhas[date(2024, 1, 1)]._criado_em == "v1"  # data de criação preservada

    # Time Travel: a versão 0 ainda tem o valor original
    v0 = spark_delta.read.format("delta").option("versionAsOf", 0).load(lh.caminho("silver", "t"))
    assert v0.first().valor == 5.0

    # A 2ª execução idêntica não deve ter alterado nenhuma linha
    ultimo = spark_delta.sql(f"DESCRIBE HISTORY {lh.nome('silver', 't')}").first()
    assert int(ultimo.operationMetrics["numTargetRowsUpdated"]) == 0
    assert int(ultimo.operationMetrics["numTargetRowsInserted"]) == 0


def test_substituir_intervalo_remove_linhas_que_sumiram_da_fonte(spark_delta, lh):
    esquema = "mes DATE, pais STRING, fob DOUBLE"
    inicial = spark_delta.createDataFrame(
        [(date(2023, 12, 1), "China", 1.0), (date(2024, 1, 1), "China", 2.0),
         (date(2024, 1, 1), "Chile", 3.0)],
        esquema,
    )  # fmt: skip
    lh.substituir_intervalo(inicial, "silver", "exp", "mes", date(2023, 12, 1), date(2024, 1, 1))

    revisado = spark_delta.createDataFrame([(date(2024, 1, 1), "China", 2.5)], esquema)
    lh.substituir_intervalo(revisado, "silver", "exp", "mes", date(2024, 1, 1), date(2024, 1, 1))

    linhas = sorted((r.mes, r.pais, r.fob) for r in lh.ler("silver", "exp").collect())
    assert linhas == [(date(2023, 12, 1), "China", 1.0), (date(2024, 1, 1), "China", 2.5)]
