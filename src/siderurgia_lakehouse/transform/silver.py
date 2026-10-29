"""Silver: interpreta o JSON da bronze, tipa, valida, deduplica. Uma tabela por assunto.

Padrão de cada fonte:
    payload JSON → campos tipados (try_cast) → regras de validação → quarentena
                 → deduplicação (versão mais recente de cada chave) → colunas de auditoria

Funções puras (DataFrame → DataFrame): testáveis sem Delta, sem rede e sem Databricks.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from siderurgia_lakehouse import config

COLUNAS_LINHAGEM = ["_id_lote", "_ingerido_em"]


# ============================================================================ utilitários
def campo(*caminhos: str) -> Column:
    """Primeiro campo não nulo do payload entre vários nomes possíveis.

    APIs públicas mudam nomes de campos sem aviso (ex.: "monthNumber" x "coMes").
    Aceitar alternativas conhecidas deixa o pipeline tolerante sem esconder o problema:
    se nenhum nome bater, o valor fica nulo e a linha cai na quarentena.
    """
    return F.coalesce(*[F.get_json_object("payload", f"$.{c}") for c in caminhos])


def para_decimal(df: DataFrame, coluna: str, precisao: str = "20,6") -> DataFrame:
    """try_cast: texto inválido vira NULL em vez de derrubar o job (com ANSI ligado,
    padrão no Spark 4 / Databricks recente, um cast comum lançaria exceção)."""
    return df.withColumn(coluna, F.expr(f"try_cast(trim({coluna}) AS DECIMAL({precisao}))"))


def para_data(df: DataFrame, coluna: str, formato: str, destino: str) -> DataFrame:
    """try_to_timestamp: data em formato inesperado vira NULL (e vai para a quarentena)."""
    return df.withColumn(destino, F.expr(f"to_date(try_to_timestamp(trim({coluna}), '{formato}'))"))


def separar_quarentena(
    df: DataFrame, regras: Sequence[tuple[Column, str]]
) -> tuple[DataFrame, DataFrame]:
    """Aplica as regras em ordem; a primeira que casar define o motivo da rejeição.

    Quarentena em vez de descarte: nada some em silêncio e dá para auditar depois.
    """
    motivo: Column | None = None
    for condicao, nome in regras:
        motivo = F.when(condicao, nome) if motivo is None else motivo.when(condicao, nome)
    marcado = df.withColumn("motivo_rejeicao", motivo if motivo is not None else F.lit(None))

    validos = marcado.filter(F.col("motivo_rejeicao").isNull()).drop("motivo_rejeicao", "payload")
    quarentena = marcado.filter(F.col("motivo_rejeicao").isNotNull()).select(
        "_fonte", "payload", "motivo_rejeicao", *COLUNAS_LINHAGEM,
        F.current_timestamp().alias("_criado_em"),
    )  # fmt: skip
    return validos, quarentena


def deduplicar(df: DataFrame, chaves: Sequence[str]) -> DataFrame:
    """Mantém a versão mais recente de cada chave.

    Duplicatas são esperadas: as cargas incrementais reprocessam períodos recentes de
    propósito. O Window com partitionBy gera um shuffle (wide transformation) —
    aceitável aqui, e é o mesmo agrupamento que o MERGE faria de qualquer forma.
    """
    janela = Window.partitionBy(*chaves).orderBy(
        F.col("_ingerido_em").desc(), F.col("_id_lote").desc()
    )
    return df.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn")


def _finalizar(df: DataFrame, colunas: Sequence[str]) -> DataFrame:
    return df.select(
        *colunas,
        *COLUNAS_LINHAGEM,
        F.current_timestamp().alias("_criado_em"),
        F.current_timestamp().alias("_atualizado_em"),
    )


# ============================================================================ IBGE / SIDRA
CHAVES_PRODUCAO = ("atividade_codigo", "metrica", "mes_referencia")


def construir_producao(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    metricas = F.create_map(
        *[x for cod, nome in config.SIDRA_VARIAVEIS.items() for x in (F.lit(str(cod)), F.lit(nome))]
    )
    df = bronze.select(
        "payload", "_fonte", *COLUNAS_LINHAGEM,
        campo("D4C").alias("atividade_codigo"),
        campo("D4N").alias("atividade_nome"),
        metricas[campo("D2C")].alias("metrica"),
        campo("D3C").alias("_mes"),
        campo("V").alias("valor"),
    )  # fmt: skip
    df = para_data(df, "_mes", "yyyyMM", "mes_referencia")
    df = para_decimal(df, "valor", "14,5")

    validos, quarentena = separar_quarentena(
        df,
        [
            # Se a variável não é uma das pedidas, a ordem das dimensões (D1..D4) mudou.
            (F.col("metrica").isNull() | F.col("atividade_codigo").isNull(), "layout_inesperado"),
            (F.col("mes_referencia").isNull(), "periodo_invalido"),
            # O IBGE usa "..", "...", "-" e "X" para valor inexistente ou sigiloso.
            (F.col("valor").isNull(), "valor_indisponivel"),
        ],
    )
    silver = _finalizar(
        deduplicar(validos, CHAVES_PRODUCAO),
        ["atividade_codigo", "atividade_nome", "metrica", "mes_referencia", "valor"],
    )
    return silver, quarentena


# ============================================================================ Comex Stat
CHAVES_EXPORTACAO = ("mes_referencia", "pais", "sh4_codigo")


def construir_exportacoes(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = bronze.select(
        "payload", "_fonte", *COLUNAS_LINHAGEM,
        campo("year", "coAno").alias("_ano"),
        campo("monthNumber", "coMes", "month").alias("_mes"),
        campo("country", "noPais", "noPaisPt").alias("pais"),
        campo("headingCode", "coSh4", "coHeading").alias("_sh4"),
        campo("heading", "noSh4Por", "noSh4").alias("sh4_descricao"),
        campo("metricFOB", "vlFob").alias("valor_fob_usd"),
        campo("metricKG", "kgLiquido").alias("peso_kg"),
    )  # fmt: skip
    df = (
        df.withColumn(
            # Se o código SH4 não vier em campo próprio, ele costuma abrir a descrição.
            "sh4_codigo",
            F.coalesce(
                F.col("_sh4"),
                F.nullif(F.regexp_extract("sh4_descricao", r"^(\d{4})", 1), F.lit("")),
            ),
        )
        .withColumn("_periodo", F.concat_ws("-", "_ano", F.lpad("_mes", 2, "0")))
        .transform(lambda d: para_data(d, "_periodo", "yyyy-MM", "mes_referencia"))
        .transform(lambda d: para_decimal(d, "valor_fob_usd", "20,2"))
        .transform(lambda d: para_decimal(d, "peso_kg", "20,3"))
    )

    validos, quarentena = separar_quarentena(
        df,
        [
            (F.col("pais").isNull() | F.col("sh4_codigo").isNull(), "layout_inesperado"),
            (F.col("mes_referencia").isNull(), "periodo_invalido"),
            (F.col("valor_fob_usd").isNull() | F.col("peso_kg").isNull(), "valor_invalido"),
        ],
    )
    silver = _finalizar(
        deduplicar(validos, CHAVES_EXPORTACAO),
        ["mes_referencia", "pais", "sh4_codigo", "sh4_descricao", "valor_fob_usd", "peso_kg"],
    )
    return silver, quarentena


# ============================================================================ Climate TRACE
CHAVES_EMISSOES = ("usina_id", "ano", "gas")


def construir_emissoes(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = bronze.select(
        "payload", "_fonte", *COLUNAS_LINHAGEM,
        campo("id", "source_id").alias("usina_id"),
        campo("name", "source_name").alias("usina_nome"),
        campo("sector").alias("setor"),
        campo("subsector").alias("subsetor"),
        campo("country", "iso3_country").alias("pais"),
        campo("centroid.latitude", "lat").alias("latitude"),
        campo("centroid.longitude", "lon").alias("longitude"),
        F.coalesce(campo("gas"), F.get_json_object("_requisicao", "$.gas")).alias("gas"),
        # Se o registro não trouxer o ano, usamos o ano que foi pedido na requisição.
        F.coalesce(campo("year"), F.get_json_object("_requisicao", "$.year")).alias("_ano"),
        campo("emissionsQuantity", "emissions_quantity").alias("emissoes_t"),
    )  # fmt: skip
    df = (
        df.withColumn("ano", F.expr("try_cast(_ano AS INT)"))
        .transform(lambda d: para_decimal(d, "emissoes_t", "20,3"))
        .transform(lambda d: para_decimal(d, "latitude", "9,6"))
        .transform(lambda d: para_decimal(d, "longitude", "9,6"))
    )
    fora_do_pais = F.col("pais").isNotNull() & (F.upper("pais") != config.CLIMATE_TRACE_PAIS)
    fora_do_setor = F.col("subsetor").isNotNull() & ~F.lower("subsetor").contains("steel")

    validos, quarentena = separar_quarentena(
        df,
        [
            (F.col("usina_id").isNull(), "layout_inesperado"),
            (F.col("ano").isNull(), "periodo_invalido"),
            # Defensivo: se a API ignorar algum filtro, a silver garante o escopo.
            (fora_do_pais | fora_do_setor, "fora_do_escopo"),
            (F.col("emissoes_t").isNull(), "valor_invalido"),
        ],
    )
    silver = _finalizar(
        deduplicar(validos, CHAVES_EMISSOES),
        ["usina_id", "usina_nome", "setor", "subsetor", "latitude", "longitude",
         "ano", "gas", "emissoes_t"],
    )  # fmt: skip
    return silver, quarentena


# ============================================================================ Banco Central
CHAVES_CAMBIO = ("data_referencia",)


def construir_cambio(bronze: DataFrame, hoje: date) -> tuple[DataFrame, DataFrame]:
    df = bronze.select(
        "payload", "_fonte", *COLUNAS_LINHAGEM,
        campo("data").alias("_data"),
        campo("valor").alias("dolar_ptax"),
    )  # fmt: skip
    df = para_data(df, "_data", "dd/MM/yyyy", "data_referencia")
    df = para_decimal(df, "dolar_ptax", "12,6")

    validos, quarentena = separar_quarentena(
        df,
        [
            (F.col("data_referencia").isNull(), "periodo_invalido"),
            (F.col("data_referencia") > F.lit(hoje), "data_futura"),
            (F.col("dolar_ptax").isNull(), "valor_invalido"),
        ],
    )
    silver = _finalizar(deduplicar(validos, CHAVES_CAMBIO), ["data_referencia", "dolar_ptax"])
    return silver, quarentena
