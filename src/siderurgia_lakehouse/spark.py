"""Criação da SparkSession — a mesma base de código roda local e no Databricks."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "siderurgia-lakehouse") -> SparkSession:
    # No Databricks a sessão já existe (com Delta, Photon e Unity Catalog configurados).
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip  # import tardio: só existe localmente

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # O volume é pequeno (milhares de linhas): o padrão de 200 partições de shuffle
        # geraria 200 tarefas quase vazias em cada groupBy/join. Ajustar evita overhead.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        # Mesmo comportamento padrão dos runtimes recentes do Databricks (Spark 4 / DBR 17+):
        # cast inválido gera erro em vez de NULL silencioso. Por isso usamos try_cast.
        .config("spark.sql.ansi.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
