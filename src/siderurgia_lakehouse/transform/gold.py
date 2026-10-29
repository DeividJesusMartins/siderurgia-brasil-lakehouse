"""Gold: modelo dimensional pronto para consumo (Power BI, Databricks SQL, notebooks).

Dimensões
    dim_atividade             — atividades industriais da PIM-PF (CNAE 2.0)
    dim_calendario            — um mês por linha
Fatos
    fato_producao_industrial  — índice de produção por atividade e mês (+ variação 12m)
    fato_exportacao_aco       — exportações de aço por mês, país de destino e posição SH4
    fato_emissoes_usina       — emissões anuais por usina siderúrgica (+ ranking, participação)
Painéis (tabelas largas, uma linha por período, para o BI ler sem joins)
    painel_siderurgia_mensal  — produção da metalurgia x exportações x câmbio
    painel_emissoes_anual     — emissões totais do setor por ano
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from siderurgia_lakehouse import config

_NOMES_MESES = [
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
]  # fmt: skip


def _idx_mes(col: str = "mes_referencia") -> Column:
    """Índice sequencial do mês (ano*12 + mês). Permite janelas com rangeBetween que
    continuam corretas mesmo se algum mês estiver faltando — rowsBetween contaria
    *linhas*, e um buraco na série deslocaria a comparação."""
    return F.year(col) * 12 + F.month(col)


def _variacao_pct(atual: Column, anterior: Column) -> Column:
    return F.round((atual / anterior - 1) * 100, 2)


# ============================================================================ dimensões
def construir_dim_atividade(producao: DataFrame) -> DataFrame:
    return producao.groupBy("atividade_codigo").agg(
        F.max_by("atividade_nome", "mes_referencia").alias("atividade_nome")
    )


def construir_dim_calendario(spark: SparkSession, inicio, fim) -> DataFrame:
    meses = F.array(*[F.lit(m) for m in _NOMES_MESES])
    return (
        spark.sql(
            f"SELECT explode(sequence(DATE'{inicio}', DATE'{fim}', INTERVAL 1 MONTH)) "
            "AS mes_referencia"
        )
        .withColumn("ano", F.year("mes_referencia"))
        .withColumn("mes", F.month("mes_referencia"))
        .withColumn("trimestre", F.quarter("mes_referencia"))
        .withColumn("semestre", F.when(F.month("mes_referencia") <= 6, 1).otherwise(2))
        .withColumn("ano_mes", F.date_format("mes_referencia", "yyyy-MM"))
        .withColumn("nome_mes", F.element_at(meses, F.month("mes_referencia")))
    )


# ============================================================================ fatos
def construir_fato_producao(producao: DataFrame) -> DataFrame:
    # "Pivot manual" com agregação condicional: o .pivot() sem lista de valores faria
    # uma passada extra nos dados só para descobrir quais métricas existem.
    largo = producao.groupBy("atividade_codigo", "mes_referencia").agg(
        *[
            F.max(F.when(F.col("metrica") == m, F.col("valor"))).cast("double").alias(m)
            for m in config.SIDRA_VARIAVEIS.values()
        ]
    )
    # Janela PARTICIONADA por atividade: cada atividade é processada independentemente,
    # em paralelo — ao contrário de uma janela sem partitionBy, que iria para 1 partição.
    w = Window.partitionBy("atividade_codigo").orderBy(_idx_mes())
    mesmo_mes_ano_anterior = F.first("indice").over(w.rangeBetween(-12, -12))
    ultimos_3 = w.rangeBetween(-2, 0)
    return largo.select(
        "atividade_codigo",
        "mes_referencia",
        F.round("indice", 2).alias("indice"),
        F.round("indice_sa", 2).alias("indice_sa"),
        _variacao_pct(F.col("indice"), mesmo_mes_ano_anterior).alias("var_12m_pct"),
        F.when(
            F.count("indice_sa").over(ultimos_3) == 3,
            F.round(F.avg("indice_sa").over(ultimos_3), 2),
        ).alias("media_movel_3m_sa"),
    )


def cambio_mensal(cambio: DataFrame) -> DataFrame:
    return cambio.groupBy(F.trunc("data_referencia", "month").alias("mes_referencia")).agg(
        F.round(F.avg("dolar_ptax"), 4).cast("double").alias("dolar_medio")
    )


def construir_fato_exportacao(exportacoes: DataFrame, cambio: DataFrame) -> DataFrame:
    # O câmbio mensal tem ~150 linhas: broadcast join manda essa tabela inteira para
    # cada executor e evita o shuffle da tabela de exportações (bem maior).
    return (
        exportacoes.join(F.broadcast(cambio_mensal(cambio)), "mes_referencia", "left")
        .withColumn("peso_t", F.col("peso_kg").cast("double") / 1000)
        .select(
            "mes_referencia",
            "pais",
            "sh4_codigo",
            "sh4_descricao",
            F.col("valor_fob_usd").cast("double").alias("valor_fob_usd"),
            F.round("peso_t", 3).alias("peso_t"),
            F.when(F.col("peso_t") > 0, F.round(F.col("valor_fob_usd") / F.col("peso_t"), 2)).alias(
                "preco_medio_usd_t"
            ),
            "dolar_medio",
            F.round(F.col("valor_fob_usd") * F.col("dolar_medio"), 2).alias("valor_fob_brl"),
        )
    )


def construir_fato_emissoes(emissoes: DataFrame) -> DataFrame:
    w_ano = Window.partitionBy("ano")
    return (
        emissoes.filter(F.col("gas") == config.CLIMATE_TRACE_GAS)
        .withColumn("emissoes_t", F.col("emissoes_t").cast("double"))
        .select(
            "usina_id",
            "usina_nome",
            "subsetor",
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            "ano",
            F.round("emissoes_t", 1).alias("emissoes_t_co2e"),
            F.dense_rank().over(w_ano.orderBy(F.desc("emissoes_t"))).alias("ranking_no_ano"),
            F.round(F.col("emissoes_t") / F.sum("emissoes_t").over(w_ano) * 100, 2).alias(
                "participacao_pct"
            ),
        )
    )


# ============================================================================ painéis
def construir_painel_mensal(
    fato_producao: DataFrame, dim_atividade: DataFrame, fato_exportacao: DataFrame
) -> DataFrame:
    nomes = fato_producao.join(F.broadcast(dim_atividade), "atividade_codigo")

    def indice_de(atividade: str, coluna: str) -> Column:
        return F.max(F.when(F.lower("atividade_nome").contains(atividade), F.col(coluna)))

    producao = nomes.groupBy("mes_referencia").agg(
        indice_de(config.ATIVIDADE_METALURGIA, "indice").alias("indice_metalurgia"),
        indice_de(config.ATIVIDADE_METALURGIA, "indice_sa").alias("indice_metalurgia_sa"),
        indice_de(config.ATIVIDADE_METALURGIA, "var_12m_pct").alias("metalurgia_var_12m_pct"),
        indice_de(config.ATIVIDADE_TRANSFORMACAO, "indice").alias("indice_transformacao"),
        indice_de(config.ATIVIDADE_INDUSTRIA_GERAL, "indice").alias("indice_industria_geral"),
    )
    exportacao = fato_exportacao.groupBy("mes_referencia").agg(
        F.round(F.sum("valor_fob_usd"), 2).alias("exportacao_fob_usd"),
        F.round(F.sum("peso_t"), 1).alias("exportacao_t"),
        F.round(F.sum("valor_fob_brl"), 2).alias("exportacao_fob_brl"),
        F.max("dolar_medio").alias("dolar_medio"),
        F.countDistinct("pais").alias("qtd_paises_destino"),
    )

    # Janela sem partitionBy leva tudo para UMA partição. Seguro aqui (≈ 140 linhas,
    # uma por mês); numa tabela grande seria um gargalo sério.
    w = Window.orderBy(_idx_mes())
    painel = producao.join(exportacao, "mes_referencia", "full_outer")
    return painel.select(
        "mes_referencia",
        F.year("mes_referencia").alias("ano"),
        F.month("mes_referencia").alias("mes"),
        "indice_metalurgia",
        "indice_metalurgia_sa",
        "metalurgia_var_12m_pct",
        "indice_transformacao",
        "indice_industria_geral",
        "exportacao_t",
        "exportacao_fob_usd",
        "exportacao_fob_brl",
        F.round(F.col("exportacao_fob_usd") / F.col("exportacao_t"), 2).alias("preco_medio_usd_t"),
        "dolar_medio",
        "qtd_paises_destino",
        _variacao_pct(
            F.col("exportacao_t"), F.first("exportacao_t").over(w.rangeBetween(-12, -12))
        ).alias("exportacao_t_var_12m_pct"),
        (F.col("mes_referencia") < F.trunc(F.current_date(), "month")).alias("mes_fechado"),
        F.current_timestamp().alias("_atualizado_em"),
    )


def construir_painel_emissoes(fato_emissoes: DataFrame) -> DataFrame:
    anual = fato_emissoes.groupBy("ano").agg(
        F.round(F.sum("emissoes_t_co2e"), 1).alias("emissoes_total_t_co2e"),
        F.countDistinct("usina_id").alias("qtd_usinas"),
        F.max_by("usina_nome", "emissoes_t_co2e").alias("maior_emissora"),
        F.max("participacao_pct").alias("participacao_maior_emissora_pct"),
    )
    w = Window.orderBy("ano")  # ~10 linhas: janela sem partição é inofensiva
    return anual.withColumn(
        "var_ano_anterior_pct",
        _variacao_pct(F.col("emissoes_total_t_co2e"), F.lag("emissoes_total_t_co2e").over(w)),
    )
