"""Camada de armazenamento: abstrai onde e como as tabelas Delta são lidas e gravadas.

- Local:      tabelas por caminho      → <base_path>/<camada>/<tabela>
- Databricks: tabelas no Unity Catalog → <catalog>.<camada>.<tabela>

O resto do código só fala em (camada, tabela) e não precisa saber em qual ambiente está.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

from siderurgia_lakehouse.config import Settings

logger = logging.getLogger(__name__)


class Lakehouse:
    def __init__(self, spark: SparkSession, settings: Settings) -> None:
        self.spark = spark
        self.settings = settings
        self.no_databricks = settings.ambiente == "databricks"

    def preparar(self) -> None:
        """Garante que schemas (e o Volume da landing) existam no Unity Catalog.

        O catálogo em si precisa ser criado antes por quem tem permissão
        (ver sql/00_setup_unity_catalog.sql). Localmente não há nada a fazer.
        """
        if not self.no_databricks:
            return
        cat = self.settings.catalog
        for camada in ("bronze", "silver", "gold"):
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{camada}")
        self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {cat}.bronze.landing")

    # ---------- identificação ----------
    def caminho(self, camada: str, tabela: str) -> str:
        return f"{self.settings.base_path}/{camada}/{tabela}"

    def nome(self, camada: str, tabela: str) -> str:
        """Identificador utilizável em spark.sql / spark.table."""
        if self.no_databricks:
            return f"{self.settings.catalog}.{camada}.{tabela}"
        return f"delta.`{self.caminho(camada, tabela)}`"

    # ---------- leitura ----------
    def existe(self, camada: str, tabela: str) -> bool:
        from delta.tables import DeltaTable

        if self.no_databricks:
            return self.spark.catalog.tableExists(self.nome(camada, tabela))
        return DeltaTable.isDeltaTable(self.spark, self.caminho(camada, tabela))

    def ler(self, camada: str, tabela: str) -> DataFrame:
        if self.no_databricks:
            return self.spark.table(self.nome(camada, tabela))
        return self.spark.read.format("delta").load(self.caminho(camada, tabela))

    def _delta_table(self, camada: str, tabela: str):
        from delta.tables import DeltaTable

        if self.no_databricks:
            return DeltaTable.forName(self.spark, self.nome(camada, tabela))
        return DeltaTable.forPath(self.spark, self.caminho(camada, tabela))

    # ---------- escrita ----------
    def _gravar(self, df: DataFrame, camada: str, tabela: str, modo: str) -> None:
        writer = df.write.format("delta").mode(modo)
        if modo == "overwrite":
            # Permite evoluir o schema da gold sem precisar dropar a tabela.
            writer = writer.option("overwriteSchema", "true")
        else:
            writer = writer.option("mergeSchema", "true")

        if self.no_databricks:
            writer.saveAsTable(self.nome(camada, tabela))
        else:
            writer.save(self.caminho(camada, tabela))
        logger.info("%s.%s gravada (modo=%s)", camada, tabela, modo)

    def anexar(self, df: DataFrame, camada: str, tabela: str) -> None:
        self._gravar(df, camada, tabela, "append")

    def sobrescrever(self, df: DataFrame, camada: str, tabela: str) -> None:
        self._gravar(df, camada, tabela, "overwrite")

    def upsert(
        self,
        df: DataFrame,
        camada: str,
        tabela: str,
        chaves: Sequence[str],
        colunas_valor: Sequence[str],
    ) -> None:
        """MERGE idempotente: insere chaves novas e só atualiza linhas cujo valor mudou.

        Rodar a mesma carga duas vezes não duplica nada nem reescreve arquivos à toa —
        a condição `whenMatchedUpdate(condition=...)` evita reescrever arquivos Delta
        quando o valor recebido é igual ao já gravado.
        """
        if not self.existe(camada, tabela):
            self._gravar(df, camada, tabela, "overwrite")
            return

        on = " AND ".join(f"t.{c} = s.{c}" for c in chaves)
        mudou = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in colunas_valor)
        atualizar = {c: f"s.{c}" for c in df.columns if c not in chaves and c != "_criado_em"}

        (
            self._delta_table(camada, tabela)
            .alias("t")
            .merge(df.alias("s"), on)
            .whenMatchedUpdate(condition=mudou, set=atualizar)
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("%s.%s atualizada via MERGE", camada, tabela)

    def substituir_intervalo(
        self, df: DataFrame, camada: str, tabela: str, coluna: str, inicio, fim
    ) -> None:
        """Sobrescreve só as linhas com `coluna` entre inicio e fim (replaceWhere do Delta).

        Ideal quando a fonte devolve um período *inteiro* consolidado (como o Comex Stat
        por ano): linhas que deixaram de existir na fonte também somem — coisa que um
        MERGE não faria — e o resto da tabela fica intocado, numa transação atômica.
        """
        if not self.existe(camada, tabela):
            self._gravar(df, camada, tabela, "overwrite")
            return
        condicao = f"{coluna} >= '{inicio}' AND {coluna} <= '{fim}'"
        writer = df.write.format("delta").mode("overwrite").option("replaceWhere", condicao)
        if self.no_databricks:
            writer.saveAsTable(self.nome(camada, tabela))
        else:
            writer.save(self.caminho(camada, tabela))
        logger.info("%s.%s: intervalo %s substituído", camada, tabela, condicao)
