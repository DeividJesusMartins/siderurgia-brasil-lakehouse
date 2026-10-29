"""Quebra de intervalos de datas em janelas menores para consultar APIs com limite de volume."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class Janela:
    inicio: date
    fim: date


def gerar_janelas(inicio: date, fim: date, anos: int) -> Iterator[Janela]:
    """Divide [inicio, fim] em janelas consecutivas de no máximo `anos` anos, sem sobreposição."""
    if anos < 1:
        raise ValueError("anos deve ser >= 1")
    atual = inicio
    while atual <= fim:
        try:
            limite = atual.replace(year=atual.year + anos) - timedelta(days=1)
        except ValueError:  # 29/02 + N anos em ano não bissexto
            limite = atual.replace(year=atual.year + anos, day=28) - timedelta(days=1)
        janela_fim = min(limite, fim)
        yield Janela(atual, janela_fim)
        atual = janela_fim + timedelta(days=1)
