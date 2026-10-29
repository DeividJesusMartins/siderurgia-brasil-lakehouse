"""Ponto de entrada (CLI). Usado localmente e como python_wheel_task no Databricks.

Exemplos:
    siderurgia-lakehouse --camada todas
    siderurgia-lakehouse --camada bronze --fontes sidra_pimpf,bcb_cambio
    siderurgia-lakehouse --camada silver --full-refresh
    siderurgia-lakehouse --camada gold --catalog siderurgia_lakehouse_dev
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

CAMADAS = ("bronze", "silver", "gold", "manutencao", "todas")
FONTES = ("sidra_pimpf", "comexstat_exportacoes", "climatetrace_usinas", "bcb_cambio")


def _args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="siderurgia-lakehouse",
        description="Pipeline medalhão com dados públicos da siderurgia brasileira.",
    )
    p.add_argument("--camada", choices=CAMADAS, default="todas")
    p.add_argument("--fontes", help=f"lista separada por vírgula (padrão: todas) — {FONTES}")
    p.add_argument("--full-refresh", action="store_true", help="reprocessa todo o histórico")
    p.add_argument("--catalog", help="catálogo do Unity Catalog (Databricks)")
    p.add_argument("--data-inicio", type=date.fromisoformat, help="AAAA-MM-DD")
    args = p.parse_args(argv)

    args.fontes = args.fontes.split(",") if args.fontes else list(FONTES)
    desconhecidas = set(args.fontes) - set(FONTES)
    if desconhecidas:
        p.error(f"fontes desconhecidas: {sorted(desconhecidas)}")
    return args


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        stream=sys.stdout,
    )
    for ruidoso in ("py4j", "urllib3"):
        logging.getLogger(ruidoso).setLevel(logging.WARNING)

    args = _args(argv)

    # Imports tardios: o --help funciona mesmo sem PySpark instalado.
    from siderurgia_lakehouse import pipeline
    from siderurgia_lakehouse.config import Settings
    from siderurgia_lakehouse.ingestion.fontes import todas_as_fontes
    from siderurgia_lakehouse.lakehouse import Lakehouse
    from siderurgia_lakehouse.spark import get_spark

    settings = Settings()
    if args.catalog:
        settings.catalog = args.catalog
    if args.data_inicio:
        settings.data_inicio = args.data_inicio

    spark = get_spark()
    lh = Lakehouse(spark, settings)
    lh.preparar()
    logging.getLogger("siderurgia_lakehouse").info(
        "Ambiente=%s | camada=%s | fontes=%s | full_refresh=%s",
        settings.ambiente, args.camada, args.fontes, args.full_refresh,
    )  # fmt: skip

    if args.camada in ("bronze", "todas"):
        fontes = [f for f in todas_as_fontes(settings) if f.nome in args.fontes]
        pipeline.executar_bronze(spark, lh, settings, fontes, args.full_refresh)
    if args.camada in ("silver", "todas"):
        pipeline.executar_silver(spark, lh, args.fontes, args.full_refresh)
    if args.camada in ("gold", "todas"):
        pipeline.executar_gold(spark, lh)
    if args.camada == "manutencao":
        pipeline.executar_manutencao(spark, lh)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
