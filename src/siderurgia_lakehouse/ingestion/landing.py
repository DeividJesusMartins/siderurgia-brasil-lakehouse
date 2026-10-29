"""Zona de pouso (landing): guarda a resposta da API exatamente como veio.

Ter o JSON original em arquivo permite reprocessar a bronze inteira sem chamar as APIs
de novo — útil quando um bug na transformação é descoberto meses depois.
No Databricks o caminho é um Volume do Unity Catalog (/Volumes/...), que se comporta
como um diretório comum para o Python.
"""

from __future__ import annotations

import json
from pathlib import Path


def salvar_lote(
    landing_path: str, fonte: str, id_lote: str, descricao: str, registros: list[dict]
) -> str:
    destino = Path(landing_path) / fonte / f"lote={id_lote}"
    destino.mkdir(parents=True, exist_ok=True)
    arquivo = destino / f"{descricao}.json"
    arquivo.write_text(json.dumps(registros, ensure_ascii=False), encoding="utf-8")
    return str(arquivo)
