"""Bronze: dado bruto + metadados de linhagem. Nenhuma regra de negócio aqui.

Todas as fontes compartilham o MESMO schema: cada registro da API vira uma linha com o
JSON original na coluna `payload`. É o padrão *schema-on-read*: a bronze aceita qualquer
formato sem quebrar, e a silver é quem interpreta os campos. Se uma API mudar o layout,
a ingestão continua funcionando e o problema aparece (de forma controlada) na silver.
"""

from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

SCHEMA_BRONZE = StructType(
    [
        StructField("payload", StringType(), False),
        StructField("_fonte", StringType(), False),
        StructField("_url", StringType(), False),
        StructField("_requisicao", StringType(), False),
        StructField("_arquivo_landing", StringType(), True),
        StructField("_id_lote", StringType(), False),
        StructField("_ingerido_em", TimestampType(), False),
    ]
)


def montar_bronze(
    spark: SparkSession,
    registros: list[dict],
    *,
    fonte: str,
    url: str,
    requisicao: dict,
    arquivo_landing: str | None,
    id_lote: str,
    ingerido_em: datetime,
) -> DataFrame:
    req = json.dumps(requisicao, ensure_ascii=False, sort_keys=True)
    linhas = [
        (json.dumps(r, ensure_ascii=False), fonte, url, req, arquivo_landing, id_lote, ingerido_em)
        for r in registros
    ]
    # Schema explícito: sem inferência (mais rápido e estável entre cargas).
    return spark.createDataFrame(linhas, SCHEMA_BRONZE)
