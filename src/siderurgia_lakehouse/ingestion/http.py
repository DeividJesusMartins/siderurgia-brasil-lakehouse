"""Cliente HTTP compartilhado pelas fontes: timeout, retry com backoff exponencial e
tratamento padronizado de erros.

- 429 / 5xx / timeout / conexão caída → erro transitório: tenta de novo, esperando
  1s, 2s, 4s, 8s... (backoff exponencial, para não martelar uma API instável).
- Outros 4xx → erro definitivo: falha na hora (tentar de novo não resolveria).
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

STATUS_RETENTAVEIS = {429, 500, 502, 503, 504}


class ApiError(RuntimeError):
    """Erro definitivo ao consultar uma API."""


class HttpClient:
    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: float = 60.0,
        max_tentativas: int = 5,
        backoff_base: float = 2.0,
        status_vazio: tuple[int, ...] = (),
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.setdefault("Accept", "application/json")
        # Identificar o cliente é boa prática com APIs públicas (e o padrão do requests
        # já vem preenchido, por isso a atribuição direta em vez de setdefault).
        self.session.headers["User-Agent"] = "siderurgia-lakehouse/1.0 (projeto de estudo)"
        self.timeout = timeout
        self.max_tentativas = max_tentativas
        self.backoff_base = backoff_base
        # Alguns endpoints respondem 404 quando simplesmente não há dados no filtro.
        self.status_vazio = status_vazio

    def get(self, url: str, params: dict | None = None) -> Any:
        return self._requisitar("GET", url, params=params)

    def post(self, url: str, json: dict, params: dict | None = None) -> Any:
        return self._requisitar("POST", url, params=params, json=json)

    def _requisitar(self, metodo: str, url: str, **kwargs) -> Any:
        ultimo_erro: Exception | None = None
        for tentativa in range(1, self.max_tentativas + 1):
            try:
                resp = self.session.request(metodo, url, timeout=self.timeout, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as exc:
                ultimo_erro = exc
            else:
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in self.status_vazio:
                    logger.warning("%s %s → HTTP %s (sem dados)", metodo, url, resp.status_code)
                    return None
                if resp.status_code not in STATUS_RETENTAVEIS:
                    raise ApiError(f"{metodo} {url}: HTTP {resp.status_code} — {resp.text[:300]}")
                ultimo_erro = ApiError(f"HTTP {resp.status_code}")

            if tentativa < self.max_tentativas:
                espera = self.backoff_base ** (tentativa - 1)
                logger.warning(
                    "%s %s: tentativa %d/%d falhou (%s). Nova tentativa em %.0fs",
                    metodo, url, tentativa, self.max_tentativas, ultimo_erro, espera,
                )  # fmt: skip
                time.sleep(espera)

        raise ApiError(
            f"{metodo} {url}: falhou após {self.max_tentativas} tentativas"
        ) from ultimo_erro
