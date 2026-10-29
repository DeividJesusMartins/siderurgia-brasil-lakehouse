from datetime import date
from decimal import Decimal

import pytest

from siderurgia_lakehouse.transform import gold


def _producao(spark, linhas):
    """linhas: (atividade_codigo, atividade_nome, metrica, mes, valor)"""
    return spark.createDataFrame(
        [(a, n, m, d, Decimal(str(v))) for a, n, m, d, v in linhas],
        "atividade_codigo STRING, atividade_nome STRING, metrica STRING, "
        "mes_referencia DATE, valor DECIMAL(14,5)",
    )


def _exportacoes(spark, linhas):
    """linhas: (mes, pais, sh4, fob_usd, kg)"""
    return spark.createDataFrame(
        [(d, p, s, "desc", Decimal(str(f)), Decimal(str(k))) for d, p, s, f, k in linhas],
        "mes_referencia DATE, pais STRING, sh4_codigo STRING, sh4_descricao STRING, "
        "valor_fob_usd DECIMAL(20,2), peso_kg DECIMAL(20,3)",
    )


def _cambio(spark, linhas):
    return spark.createDataFrame(
        [(d, Decimal(str(v))) for d, v in linhas], "data_referencia DATE, dolar_ptax DECIMAL(12,6)"
    )


def _meses(ano_ini, mes_ini, n):
    for i in range(n):
        idx = ano_ini * 12 + (mes_ini - 1) + i
        yield date(idx // 12, idx % 12 + 1, 1)


def test_fato_producao_variacao_12m_e_media_movel(spark):
    linhas = []
    for i, mes in enumerate(_meses(2023, 1, 13)):
        linhas.append(("24", "Metalurgia", "indice", mes, 100 + i))
        linhas.append(("24", "Metalurgia", "indice_sa", mes, 100 + i))
    fato = {
        r.mes_referencia: r
        for r in gold.construir_fato_producao(_producao(spark, linhas)).collect()
    }

    assert fato[date(2023, 12, 1)].var_12m_pct is None  # sem o mesmo mês do ano anterior
    assert fato[date(2024, 1, 1)].var_12m_pct == pytest.approx(12.0)  # 112 / 100 - 1
    assert fato[date(2023, 2, 1)].media_movel_3m_sa is None  # só 2 meses na janela
    assert fato[date(2023, 3, 1)].media_movel_3m_sa == pytest.approx(101.0)


def test_variacao_12m_respeita_mes_faltante(spark):
    # Sem jan/2023: a comparação de jan/2024 não pode pegar outro mês no lugar
    linhas = [
        ("24", "Metalurgia", "indice", m, 100)
        for m in _meses(2022, 12, 14)
        if m != date(2023, 1, 1)
    ]
    fato = {
        r.mes_referencia: r
        for r in gold.construir_fato_producao(_producao(spark, linhas)).collect()
    }
    assert fato[date(2024, 1, 1)].var_12m_pct is None
    assert fato[date(2023, 12, 1)].var_12m_pct == pytest.approx(0.0)


def test_fato_exportacao_preco_e_conversao_em_reais(spark):
    exp = _exportacoes(spark, [(date(2024, 1, 1), "China", "7208", 600_000, 1_000_000)])
    cambio = _cambio(spark, [(date(2024, 1, 2), 4.8), (date(2024, 1, 31), 5.2)])
    r = gold.construir_fato_exportacao(exp, cambio).first()

    assert r.peso_t == pytest.approx(1000)
    assert r.preco_medio_usd_t == pytest.approx(600)
    assert r.dolar_medio == pytest.approx(5.0)
    assert r.valor_fob_brl == pytest.approx(3_000_000)


def test_fato_emissoes_ranking_e_participacao(spark):
    emissoes = spark.createDataFrame(
        [
            ("a", "Usina A", 300, 2023, "co2e_100yr"),
            ("b", "Usina B", 100, 2023, "co2e_100yr"),
            ("a", "Usina A", 999, 2023, "ch4"),  # outro gás: fora do fato
        ],
        "usina_id STRING, usina_nome STRING, emissoes_t INT, ano INT, gas STRING",
    ).selectExpr("*", "'iron-and-steel' AS subsetor", "-20.0 AS latitude", "-40.0 AS longitude")
    fato = {r.usina_id: r for r in gold.construir_fato_emissoes(emissoes).collect()}

    assert len(fato) == 2
    assert fato["a"].ranking_no_ano == 1
    assert fato["a"].participacao_pct == pytest.approx(75.0)

    painel = gold.construir_painel_emissoes(gold.construir_fato_emissoes(emissoes)).first()
    assert painel.emissoes_total_t_co2e == pytest.approx(400)
    assert painel.maior_emissora == "Usina A"


def test_painel_mensal_identifica_metalurgia_pelo_nome(spark):
    mes = date(2024, 1, 1)
    producao = _producao(
        spark,
        [
            ("129334", "3.24 Metalurgia", "indice", mes, 95),
            ("129316", "3 Indústrias de transformação", "indice", mes, 101),
            ("129314", "1 Indústria geral", "indice", mes, 100),
        ],
    )
    fato_producao = gold.construir_fato_producao(producao)
    fato_exportacao = gold.construir_fato_exportacao(
        _exportacoes(spark, [(mes, "China", "7208", 500, 1000), (mes, "EUA", "7207", 1500, 3000)]),
        _cambio(spark, [(date(2024, 1, 5), 5.0)]),
    )
    r = gold.construir_painel_mensal(
        fato_producao, gold.construir_dim_atividade(producao), fato_exportacao
    ).first()

    assert (r.indice_metalurgia, r.indice_transformacao, r.indice_industria_geral) == (95, 101, 100)
    assert r.exportacao_t == pytest.approx(4)
    assert r.preco_medio_usd_t == pytest.approx(500)
    assert r.qtd_paises_destino == 2


def test_dim_calendario_mensal(spark):
    cal = gold.construir_dim_calendario(spark, date(2023, 11, 1), date(2024, 2, 1)).collect()
    assert [r.ano_mes for r in cal] == ["2023-11", "2023-12", "2024-01", "2024-02"]
    assert cal[2].nome_mes == "janeiro" and cal[2].semestre == 1
