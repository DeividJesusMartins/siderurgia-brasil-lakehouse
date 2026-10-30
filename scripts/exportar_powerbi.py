"""Exporta as tabelas da camada gold para arquivos Parquet únicos, prontos para o Power BI.

O relatório em powerbi/SiderurgiaBrasil.pbip lê esses arquivos a partir do parâmetro
`PastaDados` (Power Query). Uso, depois de rodar o pipeline:

    python scripts/exportar_powerbi.py                 # gera ./exports e gold_powerbi.zip
    python scripts/exportar_powerbi.py --destino C:/pasta/qualquer

Usa só o Spark (sem pandas): cada tabela é gravada com coalesce(1) e o arquivo
part-*.parquet resultante é renomeado para <tabela>.parquet.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

from siderurgia_lakehouse.config import Settings
from siderurgia_lakehouse.lakehouse import Lakehouse
from siderurgia_lakehouse.spark import get_spark

TABELAS_GOLD = [
    "dim_atividade",
    "dim_calendario",
    "fato_producao_industrial",
    "fato_exportacao_aco",
    "fato_emissoes_usina",
    "painel_siderurgia_mensal",
    "painel_emissoes_anual",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--destino", default="exports", help="pasta de saída (padrão: ./exports)")
    parser.add_argument("--sem-zip", action="store_true", help="não gera o gold_powerbi.zip")
    args = parser.parse_args()

    spark = get_spark("exportar-powerbi")
    spark.sparkContext.setLogLevel("ERROR")
    lh = Lakehouse(spark, Settings())
    destino = Path(args.destino)
    destino.mkdir(parents=True, exist_ok=True)

    for tabela in TABELAS_GOLD:
        if not lh.existe("gold", tabela):
            print(f"--  {tabela:28s} não existe na gold (rode o pipeline antes)")
            continue
        df = lh.ler("gold", tabela)
        df = df.drop(*[c for c in df.columns if c.startswith("_")])  # colunas técnicas
        with tempfile.TemporaryDirectory() as tmp:
            df.coalesce(1).write.mode("overwrite").parquet(tmp + "/saida")
            parte = next(Path(tmp, "saida").glob("part-*.parquet"))
            shutil.move(str(parte), destino / f"{tabela}.parquet")
        print(f"OK  {tabela:28s} {df.count():>7} linhas")

    if not args.sem_zip:
        arquivo = shutil.make_archive("gold_powerbi", "zip", destino)
        print(f"\nArquivo pronto: {arquivo}")


if __name__ == "__main__":
    main()
