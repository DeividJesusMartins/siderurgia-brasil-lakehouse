import json
from datetime import date

import pytest
import responses

from conftest import comex, sidra, usina
from siderurgia_lakehouse.config import Settings
from siderurgia_lakehouse.ingestion.fontes import (
    BcbCambio,
    ClimateTraceUsinas,
    ComexStatExportacoes,
    SidraPimPf,
)
from siderurgia_lakehouse.ingestion.http import ApiError, HttpClient
from siderurgia_lakehouse.ingestion.janelas import Janela, gerar_janelas

HOJE = date(2026, 10, 15)


@pytest.fixture
def settings(tmp_path):
    return Settings(ambiente="local", base_path=str(tmp_path), data_inicio=date(2015, 1, 1))


@pytest.fixture
def http():
    return HttpClient(backoff_base=0, status_vazio=(404,))


# ----------------------------------------------------------------------------- janelas
def test_janelas_contiguas_sem_sobreposicao():
    assert list(gerar_janelas(date(2015, 1, 1), date(2021, 6, 30), anos=3)) == [
        Janela(date(2015, 1, 1), date(2017, 12, 31)),
        Janela(date(2018, 1, 1), date(2020, 12, 31)),
        Janela(date(2021, 1, 1), date(2021, 6, 30)),
    ]


def test_janela_29_de_fevereiro():
    janelas = list(gerar_janelas(date(2020, 2, 29), date(2022, 1, 1), anos=1))
    assert janelas[0] == Janela(date(2020, 2, 29), date(2021, 2, 27))


# ----------------------------------------------------------------------------- SIDRA
@responses.activate
def test_sidra_carga_completa_remove_cabecalho(settings, http):
    cabecalho = {"V": "Valor", "D1C": "Brasil (Código)", "D3C": "Mês (Código)"}
    responses.get(
        "https://apisidra.ibge.gov.br/values/t/8888/n1/all/v/12606,12607/p/all/c544/all",
        json=[cabecalho, sidra("202401", "98.1")],
    )
    lote = next(SidraPimPf(settings, http).extrair(None, HOJE))
    assert len(lote.registros) == 1
    assert lote.registros[0]["V"] == "98.1"


@responses.activate
def test_sidra_incremental_pede_ultimos_meses(settings, http):
    responses.get(
        "https://apisidra.ibge.gov.br/values/t/8888/n1/all/v/12606,12607/p/last%2024/c544/all",
        json=[{"V": "Valor"}],
    )
    lote = next(SidraPimPf(settings, http).extrair(date(2026, 8, 1), HOJE))
    assert lote.registros == []
    assert lote.descricao == "last_24"


# ----------------------------------------------------------------------------- Comex Stat
def test_comex_anos_carga_completa_e_incremental(settings):
    fonte = ComexStatExportacoes(settings)
    assert fonte.anos(None, HOJE) == range(2015, 2027)
    assert fonte.anos(date(2026, 9, 1), HOJE) == range(2025, 2027)  # reprocessa 2 anos
    assert fonte.anos(date(2022, 3, 1), HOJE) == range(2022, 2027)  # marca antiga


def test_comex_agrupa_anos_em_blocos(settings):
    fonte = ComexStatExportacoes(settings)
    assert fonte.blocos(None, HOJE) == [range(2015, 2019), range(2019, 2023), range(2023, 2027)]
    assert fonte.blocos(date(2026, 9, 1), HOJE) == [range(2025, 2027)]


@responses.activate
def test_comex_monta_corpo_e_le_envelope(settings, http):
    url = "https://api-comexstat.mdic.gov.br/general"
    responses.post(url, json={"data": {"list": [comex(2026, 1, "China", 100, 200)]}})

    lotes = list(ComexStatExportacoes(settings, http).extrair(date(2026, 9, 1), HOJE))
    assert [(lote.descricao, len(lote.registros)) for lote in lotes] == [("2025_2026", 1)]

    corpo = json.loads(responses.calls[0].request.body)
    assert corpo["flow"] == "export"
    assert corpo["period"] == {"from": "2025-01", "to": "2026-10"}  # até o mês atual
    assert corpo["filters"] == [{"filter": "chapter", "values": ["72"]}]
    assert "language=pt" in responses.calls[0].request.url


def test_comex_rejeita_formato_desconhecido():
    with pytest.raises(ApiError, match="formato de resposta inesperado"):
        ComexStatExportacoes._extrair_lista({"resultado": 1})


# ----------------------------------------------------------------------------- Climate TRACE
@responses.activate
def test_climate_trace_pagina_ate_pagina_incompleta(settings, http):
    fonte = ClimateTraceUsinas(settings, http)
    fonte.por_pagina = 2
    url = "https://api.climatetrace.org/v7/sources"
    responses.get(url, json=[usina("1", "A", 10), usina("2", "B", 20)])
    responses.get(url, json=[usina("3", "C", 30)])

    lote = next(fonte.extrair(date(2025, 12, 31), date(2026, 3, 1)))
    assert lote.descricao == "2023"  # incremental: últimos 3 anos fechados (2023-2025)
    assert len(lote.registros) == 3
    assert len(responses.calls) == 2
    assert "offset=2" in responses.calls[1].request.url
    assert "subsectors=iron-and-steel" in responses.calls[0].request.url
    assert "gadmId=BRA" in responses.calls[0].request.url


def test_climate_trace_comeca_no_primeiro_ano_com_dados(settings):
    assert ClimateTraceUsinas(settings).anos(None, HOJE) == range(2021, 2026)


def test_climate_trace_aceita_lista_dentro_de_objeto():
    assert ClimateTraceUsinas._extrair_lista({"sources": [{"id": 1}]}) == [{"id": 1}]
    assert ClimateTraceUsinas._extrair_lista(None) == []


# ----------------------------------------------------------------------------- BCB
@responses.activate
def test_bcb_incremental_volta_lookback(settings, http):
    responses.get(
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados",
        json=[{"data": "01/10/2026", "valor": "5.40"}],
    )
    lotes = list(BcbCambio(settings, http).extrair(date(2026, 10, 1), HOJE))
    params = responses.calls[0].request.params
    assert params["dataInicial"] == "17/08/2026"  # 01/10 - 45 dias
    assert params["dataFinal"] == "15/10/2026"
    assert len(lotes) == 1
