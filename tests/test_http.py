import pytest
import responses

from siderurgia_lakehouse.ingestion.http import ApiError, HttpClient

URL = "https://api.exemplo.gov.br/dados"


@pytest.fixture
def http():
    return HttpClient(backoff_base=0)  # sem espera real entre tentativas


@responses.activate
def test_retry_em_erro_transitorio(http):
    responses.get(URL, status=503)
    responses.get(URL, status=429)
    responses.get(URL, json=[1, 2])
    assert http.get(URL) == [1, 2]
    assert len(responses.calls) == 3


@responses.activate
def test_desiste_apos_max_tentativas(http):
    responses.get(URL, status=500)
    with pytest.raises(ApiError, match="falhou após 5 tentativas"):
        http.get(URL)
    assert len(responses.calls) == 5


@responses.activate
def test_erro_definitivo_nao_tenta_de_novo(http):
    responses.post(URL, status=400, json={"error": {"code": 400, "message": "Filtro inválido"}})
    with pytest.raises(ApiError, match="HTTP 400"):
        http.post(URL, json={})
    assert len(responses.calls) == 1


@responses.activate
def test_status_vazio_vira_none():
    responses.get(URL, status=404)
    assert HttpClient(backoff_base=0, status_vazio=(404,)).get(URL) is None
