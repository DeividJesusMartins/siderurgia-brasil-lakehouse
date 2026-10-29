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


@responses.activate
def test_429_respeita_o_tempo_pedido_pela_api(monkeypatch):
    esperas = []
    monkeypatch.setattr("siderurgia_lakehouse.ingestion.http.time.sleep", esperas.append)
    mensagem = "Você excedeu o limite de solicitações. Por favor, tente novamente em 10 segundos."
    responses.post(URL, status=429, json={"error": {"code": 429, "message": mensagem}})
    responses.post(URL, status=429, headers={"Retry-After": "30"})
    responses.post(URL, json={"ok": True})

    assert HttpClient(backoff_base=2, folga_429=2).post(URL, json={}) == {"ok": True}
    assert esperas == [12, 32]  # 10 s pedidos + folga; depois o Retry-After (30) + folga


@responses.activate
def test_pausa_entre_requisicoes(monkeypatch):
    esperas = []
    monkeypatch.setattr("siderurgia_lakehouse.ingestion.http.time.sleep", esperas.append)
    responses.get(URL, json=[])
    http = HttpClient(pausa_entre_requisicoes=12)
    http.get(URL)
    http.get(URL)
    assert len(esperas) == 1 and 11 < esperas[0] <= 12  # só a 2ª chamada espera
