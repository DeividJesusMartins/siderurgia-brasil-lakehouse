import pytest

from siderurgia_lakehouse import quality as dq


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [(1, "a", 10.0), (1, "a", 20.0), (2, None, 999.0)], "id INT, cat STRING, v DOUBLE"
    )


def test_nao_nulo(df):
    assert dq.nao_nulo("cat")(df).violacoes == 1
    assert dq.nao_nulo("id")(df).passou


def test_unico(df):
    assert dq.unico("id")(df).violacoes == 1  # id=1 aparece duas vezes
    assert dq.unico("id", "v")(df).passou


def test_entre_com_filtro(df):
    assert dq.entre("v", 0, 100)(df).violacoes == 1
    assert dq.entre("v", 0, 100, filtro="cat IS NOT NULL")(df).passou


def test_nao_vazio(spark, df):
    assert dq.nao_vazio()(df).passou
    assert not dq.nao_vazio()(df.limit(0)).passou


def test_erro_interrompe_e_alerta_nao(df):
    # alerta: só registra
    dq.executar(df, [dq.entre("v", 0, 100, severidade="alerta")], "t")
    # erro: lança exceção
    with pytest.raises(dq.DataQualityError, match="unico"):
        dq.executar(df, [dq.unico("id")], "t")
