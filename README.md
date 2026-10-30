# 🏭 Siderurgia Brasil Lakehouse

[![CI](https://github.com/DeividJesusMartins/siderurgia-brasil-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/DeividJesusMartins/siderurgia-brasil-lakehouse/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2-00ADD4)
![Databricks](https://img.shields.io/badge/Databricks-Asset_Bundles-FF3621?logo=databricks&logoColor=white)

Pipeline de dados ponta a ponta sobre a **siderurgia brasileira**. Ele integra **quatro APIs públicas** com formatos diferentes:
- produção industrial do **IBGE**;
- exportações de aço do **MDIC**;
- emissões de gases de efeito estufa por usina do **Climate TRACE**;
- câmbio do **Banco Central**.

Os dados passam por uma **arquitetura medalhão (bronze → silver → gold)** com **PySpark + Delta Lake** e saem num modelo dimensional pronto para Power BI ou Databricks SQL.

O mesmo código roda **na sua máquina** (Delta por caminho) e **no Databricks** (Unity Catalog + Jobs serverless). O projeto inclui testes automatizados, CI/CD e infraestrutura como código.

> Todos os dados são **públicos e oficiais**. Nenhum dado de empresa é usado.

---

## 🎯 Perguntas de negócio

1. **Como evolui a produção da metalurgia** em relação ao restante da indústria de transformação?
2. **O câmbio puxa as exportações de aço?** O que acontece com volume e preço por tonelada quando o dólar sobe?
3. **Quem são os maiores emissores do setor** e quão concentradas estão as emissões entre as usinas?

## 🏗️ Arquitetura

```mermaid
flowchart LR
    subgraph APIs públicas
        A1[IBGE SIDRA<br/>PIM-PF]
        A2[MDIC<br/>Comex Stat]
        A3[Climate TRACE<br/>v7 beta]
        A4[Banco Central<br/>SGS]
    end

    A1 & A2 & A3 & A4 -->|HTTP + retry<br/>em paralelo| L[/Landing<br/>JSON bruto/]
    L --> B[(🥉 Bronze<br/>1 tabela por fonte<br/>payload JSON · append)]
    B -->|parse · tipagem<br/>validação · dedup| S[(🥈 Silver<br/>1 tabela por assunto<br/>MERGE / replaceWhere)]
    B -.->|rejeitados| Q[(Quarentena)]
    S -->|modelagem<br/>janelas 12m| G[(🥇 Gold<br/>dimensões · fatos · painéis)]
    G --> BI[Power BI /<br/>Databricks SQL]
    DQ{{Data Quality}} -.-> S & G
```

**Job no Databricks:** as 4 ingestões rodam em paralelo, então uma API lenta não atrasa as outras. Cada silver depende só da sua bronze, e a gold espera todas.

| Camada | Estratégia | Por quê |
|---|---|---|
| Landing | arquivo JSON por fonte/lote | Reprocessar sem chamar as APIs de novo |
| Bronze | `append`, schema único (`payload` JSON + linhagem) | *Schema-on-read*: se uma API mudar o formato, a ingestão não quebra |
| Silver — IBGE, Climate TRACE, BCB | `MERGE` idempotente | Absorve revisões sem duplicar; só reescreve linhas que mudaram |
| Silver — Comex Stat | `replaceWhere` do período | A fonte devolve o ano consolidado; linhas que sumiram da fonte também somem |
| Gold | `overwrite` | Volume pequeno; métricas de 12 meses dependem do histórico |

Detalhes de cada tabela e coluna em [`docs/modelo_de_dados.md`](docs/modelo_de_dados.md).

## ⚙️ O que o projeto demonstra

**Ingestão de fontes heterogêneas**
- Quatro estilos de API:
  - **SIDRA**: parâmetros no caminho da URL e resposta com uma linha de cabeçalho.
  - **Comex Stat**: consulta via `POST` com corpo JSON e limite de linhas.
  - **Climate TRACE**: paginação por `limit`/`offset`.
  - **BCB**: janela máxima de 10 anos por consulta.
- Um contrato único (`Fonte.extrair`) faz o pipeline tratar as quatro do mesmo jeito.
- Cliente HTTP compartilhado com *retry* e *backoff* exponencial para 429/5xx. Erros definitivos falham na hora.
- **Carga incremental por marca d'água** com janela de reprocessamento específica de cada fonte. As fontes revisam dados já publicados: o IBGE revisa a PIM-PF e o MDIC consolida os meses recentes.
- **Isolamento de falhas**: se uma API cair, as outras são ingeridas e o erro é reportado no fim.

**Transformação e modelagem**
- Bronze *schema-on-read* e silver tolerante a variações de nome de campo. Um layout desconhecido vai para a **quarentena** com o motivo, sem derrubar o job.
- `try_cast` e `try_to_timestamp` são seguros com ANSI ligado, que é o padrão no Spark 4 e no Databricks recente. Eles tratam os símbolos do IBGE (`..`, `X`).
- Variação de 12 meses com `rangeBetween` sobre um índice de mês. Ela continua correta mesmo com meses faltantes, algo que `lag` e `rowsBetween` não garantem.
- Janelas **particionadas** (por atividade, por ano) contra janelas globais, com o custo de cada uma comentado no código.
- *Broadcast joins* para dimensões pequenas e "pivot manual" com agregação condicional.

**Qualidade e confiabilidade**
- Checagens de *data quality* entre camadas: não nulo, unicidade e faixas de sanidade. Severidade `erro` bloqueia a publicação.
- 38 testes:
  - clientes HTTP simulados;
  - parsing de cada fonte;
  - modelagem;
  - fluxo ponta a ponta com carga incremental, revisão de dados e falha de uma fonte;
  - integração com Delta real (MERGE, replaceWhere e Time Travel).

**Operação (DataOps)**
- **Databricks Asset Bundle**: DAG de 9 tasks em serverless, com retry por fonte, alerta por e-mail e job semanal de `OPTIMIZE ZORDER` + `VACUUM`.
- **Unity Catalog**: catálogos por ambiente (`_dev` / prod), schemas por camada, Volume para a landing e `GRANT` por grupo.
- **CI** (GitHub Actions): ruff, pytest, testes Delta e build do wheel. **CD**: `bundle deploy -t prod` depois que a CI passa na `main`.

## 📁 Estrutura

```
siderurgia-brasil-lakehouse/
├── src/siderurgia_lakehouse/
│   ├── config.py              # parâmetros das fontes e do ambiente
│   ├── spark.py               # SparkSession local (Delta) ou Databricks
│   ├── lakehouse.py           # leitura/escrita/MERGE/replaceWhere: local ou Unity Catalog
│   ├── ingestion/
│   │   ├── http.py            # cliente HTTP com retry e backoff
│   │   ├── fontes.py          # SIDRA, Comex Stat, Climate TRACE, BCB
│   │   ├── janelas.py         # quebra de períodos longos
│   │   └── landing.py         # persistência do JSON bruto
│   ├── transform/
│   │   ├── bronze.py          # schema único com payload + linhagem
│   │   ├── silver.py          # parsing, validação, quarentena, deduplicação
│   │   └── gold.py            # dimensões, fatos e painéis
│   ├── quality.py             # checagens de data quality
│   ├── pipeline.py            # orquestração das camadas
│   └── main.py                # CLI (entry point do wheel)
├── tests/                     # pytest: unitários, fluxo, integração Delta
├── resources/jobs.yml         # Jobs do Databricks (DAG)
├── databricks.yml             # Asset Bundle (targets dev/prod)
├── sql/                       # setup do Unity Catalog + consultas analíticas
├── scripts/exportar_powerbi.py # exporta a gold em Parquet para o Power BI
├── powerbi/                   # dashboard Power BI (PBIP: TMDL + PBIR)
├── docs/
│   ├── modelo_de_dados.md     # dicionário de dados
│   ├── dashboard.md           # guia do dashboard
│   └── img/                   # logo do projeto
├── .github/workflows/         # CI e deploy
└── Makefile
```

## 🚀 Como executar

### Localmente

Pré-requisitos: Python 3.10+ e Java 17 (o Spark roda na JVM).

```bash
git clone https://github.com/DeividJesusMartins/siderurgia-brasil-lakehouse.git
cd siderurgia-brasil-lakehouse
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
make install          # ou: pip install -e ".[dev,spark]"

make test             # testes rápidos (sem rede, sem Delta)
make run              # pipeline completo → ./lakehouse/{landing,bronze,silver,gold}
```

A primeira execução baixa o histórico desde 2015 e leva alguns minutos. As seguintes são incrementais.

```bash
python -m siderurgia_lakehouse --camada bronze --fontes sidra_pimpf,bcb_cambio
python -m siderurgia_lakehouse --camada todas --full-refresh
make fonte F=comexstat_exportacoes        # bronze + silver de uma fonte só
python -m siderurgia_lakehouse --help
```

### No Databricks

```bash
# 1. Uma vez: crie os catálogos (sql/00_setup_unity_catalog.sql) e ajuste o host em databricks.yml
databricks auth login --host https://SEU-WORKSPACE.cloud.databricks.com

# 2. Publique e rode
databricks bundle deploy -t dev
databricks bundle run siderurgia_lakehouse_diario -t dev
```

Depois, explore com as consultas de [`sql/01_consultas_gold.sql`](sql/01_consultas_gold.sql) ou conecte o Power BI no SQL Warehouse em `siderurgia_lakehouse.gold`.

## 📈 Dashboard Power BI

Relatório em **PBIP** (versionável no Git) com 4 páginas — **Visão Geral, Produção, Exportações e Emissões** — sobre a camada gold:
- KPIs de 12 meses com variação (▲/▼) e data de referência;
- produção dessazonalizada da metalurgia × indústria, exportações × câmbio, top destinos/produtos e ranking de emissões por usina;
- **tradução pt / en / es** dirigida por tabelas e medidas, navegação em pílulas e filtros por página.

```bash
python scripts/exportar_powerbi.py   # gera os .parquet da gold em ./exports
```
Depois, abra `powerbi/SiderurgiaBrasil.pbip`, aponte o parâmetro `PastaDados` para a pasta dos `.parquet` e clique em **Atualizar**. Detalhes em [`docs/dashboard.md`](docs/dashboard.md).

## 📊 Fontes

| Fonte | Endpoint | Recorte |
|---|---|---|
| [IBGE — PIM-PF](https://sidra.ibge.gov.br/tabela/8888) | `apisidra.ibge.gov.br/values/t/8888/...` | Índices de produção por atividade (base 2022=100) |
| [MDIC — Comex Stat](https://comexstat.mdic.gov.br) | `POST api-comexstat.mdic.gov.br/general` | Exportações do capítulo 72 (ferro e aço) por país e SH4 |
| [Climate TRACE](https://climatetrace.org/data) | `GET api.climatetrace.org/v7/sources` | Usinas de ferro e aço no Brasil, t CO₂e/ano |
| [Banco Central — SGS](https://www3.bcb.gov.br/sgspub) | `api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados` | Dólar PTAX venda |

**Pontos de atenção conhecidos.** O código já se protege contra eles:
- **Comex Stat**: a documentação oficial (`/docs`) mostra o corpo da requisição, mas não o formato da resposta. A silver aceita os nomes de campo conhecidos (`metricFOB`/`vlFob`, `monthNumber`/`coMes`...). Se aparecer algo diferente, os registros vão para a quarentena com `layout_inesperado`.
- **Climate TRACE**: a API v7 é *beta* e pede uso moderado. A ingestão é sequencial e tem limite de páginas. A silver também filtra país e subsetor caso a API ignore algum parâmetro. Sem essa fonte, a gold é publicada mesmo assim, só sem as tabelas de emissões.

## 🧠 Decisões de projeto

- **Por que bronze com `payload` JSON em vez de colunas?** São quatro APIs fora do meu controle. Com o JSON bruto, uma mudança de layout nunca quebra a ingestão. O problema aparece de forma controlada na silver, que pode ser corrigida e reprocessada a partir da bronze.
- **Por que MERGE em umas fontes e replaceWhere em outra?** IBGE, Climate TRACE e BCB revisam *valores* de chaves que continuam existindo, e o MERGE resolve isso. O Comex Stat devolve o *período inteiro* consolidado, e uma combinação país/produto pode desaparecer depois da consolidação. Só substituindo o intervalo essa linha some da silver.
- **Por que identificar a metalurgia pelo nome, não pelo código?** O IBGE já renumerou as tabelas da PIM-PF (a 8159 virou a 8888 em 2023). Nomes de atividade da CNAE são mais estáveis que códigos internos de categoria.
- **Por que não particionar as tabelas?** Com dezenas de milhares de linhas, partições gerariam arquivos minúsculos. A manutenção semanal usa `ZORDER`. Em volume maior, o caminho seria *Liquid Clustering*.
- **Por que PySpark para esse volume?** Para demonstrar o padrão que escala. O mesmo código processaria milhões de linhas sem mudar a arquitetura.

## 🔭 Próximos passos

- [x] Dashboard no Power BI sobre a gold (PBIP, pt/en/es)
- [ ] Versão do dashboard em Databricks AI/BI
- [ ] Importações de aço (mesma API, `flow: import`) e balança comercial do setor
- [ ] Migrar a silver para Lakeflow Declarative Pipelines com *expectations* nativas
- [ ] Produção física em toneladas (Instituto Aço Brasil) para calcular intensidade de emissões

## 👤 Autor

**Deivid Martins** — Analista de Dados com foco em Engenharia de Dados (Databricks, PySpark, SQL, Power BI), atuando em projetos de dados para o setor siderúrgico.
[LinkedIn](https://www.linkedin.com/in/deividmartins) · [GitHub](https://github.com/DeividJesusMartins)

Licença [MIT](LICENSE).
