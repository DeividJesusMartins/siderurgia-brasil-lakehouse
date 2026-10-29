"""Teste ponta a ponta da orquestração (bronze → silver → gold) sem rede e sem Delta.

As APIs são substituídas por fontes falsas e o Lakehouse por uma versão em memória.
Isso valida o *fluxo* (marca d'água, incremental, MERGE x replaceWhere, quarentena,
isolamento de falhas, DQ). A semântica real do Delta é coberta por test_lakehouse_delta.py.
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F

from conftest import comex, sidra, usina
from siderurgia_lakehouse import pipeline
from siderurgia_lakehouse.config import Settings
from siderurgia_lakehouse.ingestion.fontes import Lote

HOJE = date(2025, 3, 10)


class FonteFalsa:
    def __init__(self, nome, lotes, erro=None):
        self.nome, self.url = nome, f"fake://{nome}"
        self.lotes, self.erro = lotes, erro
        self.marcas: list = []

    def extrair(self, marca_dagua, hoje):
        self.marcas.append(marca_dagua)
        if self.erro:
            raise self.erro
        yield from self.lotes


class LakehouseMemoria:
    def __init__(self):
        self.tabelas = {}

    def existe(self, camada, tabela):
        return (camada, tabela) in self.tabelas

    def ler(self, camada, tabela):
        return self.tabelas[(camada, tabela)]

    def anexar(self, df, camada, tabela):
        atual = self.tabelas.get((camada, tabela))
        df = df.localCheckpoint()
        self.tabelas[(camada, tabela)] = df if atual is None else atual.unionByName(df)

    def sobrescrever(self, df, camada, tabela):
        self.tabelas[(camada, tabela)] = df.localCheckpoint()

    def upsert(self, df, camada, tabela, chaves, colunas_valor):
        uniao = (
            self.ler(camada, tabela)
            .withColumn("_p", F.lit(0))
            .unionByName(df.withColumn("_p", F.lit(1)))
        )
        w = Window.partitionBy(*chaves).orderBy(F.desc("_p"))
        self.sobrescrever(
            uniao.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_p"),
            camada, tabela,
        )  # fmt: skip

    def substituir_intervalo(self, df, camada, tabela, coluna, inicio, fim):
        fora = self.ler(camada, tabela).filter(~F.col(coluna).between(inicio, fim))
        self.sobrescrever(fora.unionByName(df), camada, tabela)


def _meses(n, ano=2023):
    return [f"{ano + i // 12}{i % 12 + 1:02d}" for i in range(n)]


@pytest.fixture
def settings(tmp_path):
    return Settings(ambiente="local", base_path=str(tmp_path), data_inicio=date(2023, 1, 1))


def _fontes():
    producao = [
        r
        for i, mes in enumerate(_meses(14))
        for r in (
            sidra(mes, str(100 + i)),
            sidra(mes, str(100 + i), variavel="12607"),
            sidra(mes, "101", atividade=("129316", "3 Indústrias de transformação")),
        )
    ]
    producao.append(sidra("202402", "..."))  # → quarentena
    exportacoes = [
        Lote("2023", {}, [comex(2023, m, "China", 1000 * m, 2000 * m) for m in range(1, 13)]),
        Lote("2024", {}, [comex(2024, 1, "China", 600, 1000), comex(2024, 1, "Chile", 50, 100)]),
    ]
    return [
        FonteFalsa("sidra_pimpf", [Lote("all", {}, producao)]),
        FonteFalsa("comexstat_exportacoes", exportacoes),
        FonteFalsa(
            "climatetrace_usinas",
            [
                Lote(
                    "2023",
                    {"year": 2023},
                    [
                        usina("u1", "Usina A", 300.0),
                        usina("u2", "Usina B", 100.0),
                    ],
                )
            ],
        ),
        FonteFalsa(
            "bcb_cambio",
            [
                Lote(
                    "x",
                    {},
                    [
                        {"data": "02/01/2024", "valor": "4.9"},
                        {"data": "31/01/2024", "valor": "5.1"},
                    ],
                )
            ],
        ),
    ]


def test_fluxo_completo_e_incremental(spark, settings):
    lh = LakehouseMemoria()
    fontes = _fontes()

    # ---------------------------------------------------------------- 1ª execução
    totais = pipeline.executar_bronze(spark, lh, settings, fontes, hoje=HOJE)
    assert totais == {"sidra_pimpf": 43, "comexstat_exportacoes": 14,
                      "climatetrace_usinas": 2, "bcb_cambio": 2}  # fmt: skip
    assert all(f.marcas == [None] for f in fontes)  # sem silver ainda → carga completa

    pipeline.executar_silver(spark, lh, hoje=HOJE)
    assert lh.ler("silver", "producao_industrial").count() == 42
    assert lh.ler("silver", "quarentena").first().motivo_rejeicao == "valor_indisponivel"

    gold = pipeline.executar_gold(spark, lh)
    assert gold["painel_emissoes_anual"] == 1
    jan24 = (
        lh.ler("gold", "painel_siderurgia_mensal").filter("mes_referencia = '2024-01-01'").first()
    )
    assert jan24.metalurgia_var_12m_pct == pytest.approx(12.0)  # 112 / 100
    assert jan24.exportacao_t == pytest.approx(1.1)
    assert jan24.dolar_medio == pytest.approx(5.0)

    # ---------------------------------------------------------------- 2ª execução
    # O MDIC consolida 2024: o Chile some e a China é revista. O IBGE revisa jan/2024.
    fontes[0].lotes = [Lote("last_24", {}, [sidra("202401", "113")])]
    fontes[1].lotes = [Lote("2024", {}, [comex(2024, 1, "China", 700, 1000)])]
    for f in fontes[2:]:
        f.lotes = []

    pipeline.executar_bronze(spark, lh, settings, fontes, hoje=HOJE)
    assert fontes[0].marcas[-1] == date(2024, 2, 1)  # marca d'água = último mês na silver
    assert fontes[2].marcas[-1] == date(2023, 12, 31)  # período anual

    pipeline.executar_silver(spark, lh, hoje=HOJE)
    exp = lh.ler("silver", "exportacoes_aco")
    assert exp.count() == 13  # 12 meses de 2023 intactos + só a China em jan/2024
    assert float(exp.filter("mes_referencia = '2024-01-01'").first().valor_fob_usd) == 700

    revisto = lh.ler("silver", "producao_industrial").filter(
        "mes_referencia = '2024-01-01' AND metrica = 'indice' AND atividade_codigo = '129334'"
    )
    assert float(revisto.first().valor) == 113


def test_falha_em_uma_fonte_nao_impede_as_outras(spark, settings):
    lh = LakehouseMemoria()
    fontes = _fontes()
    fontes[2].erro = RuntimeError("API fora do ar")

    with pytest.raises(pipeline.PipelineError, match="climatetrace_usinas"):
        pipeline.executar_bronze(spark, lh, settings, fontes, hoje=HOJE)

    assert lh.existe("bronze", "sidra_pimpf")
    assert lh.existe("bronze", "bcb_cambio")
    assert not lh.existe("bronze", "climatetrace_usinas")

    # Sem emissões, a gold é publicada mesmo assim (Climate TRACE é opcional)
    pipeline.executar_silver(spark, lh, ["sidra_pimpf", "comexstat_exportacoes", "bcb_cambio"],
                             hoje=HOJE)  # fmt: skip
    assert "fato_emissoes_usina" not in pipeline.executar_gold(spark, lh)
