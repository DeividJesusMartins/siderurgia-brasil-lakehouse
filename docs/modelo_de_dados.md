# Dicionário de dados

## Fontes

| Fonte | API | O que traz | Granularidade |
|---|---|---|---|
| `sidra_pimpf` | IBGE SIDRA — tabela 8888 (PIM-PF, base 2022=100) | Índice de produção física por atividade industrial (CNAE 2.0) | mensal, Brasil |
| `comexstat_exportacoes` | MDIC Comex Stat — `POST /general` | Exportações do capítulo 72 do SH (ferro e aço): US$ FOB e kg | mensal × país × SH4 |
| `climatetrace_usinas` | Climate TRACE — `GET /v7/sources` (beta) | Emissões estimadas por usina siderúrgica no Brasil, em t CO₂e (GWP 100 anos) | anual × usina |
| `bcb_cambio` | Banco Central — SGS série 1 | Dólar PTAX venda | diária |

## Bronze — uma tabela por fonte, mesmo schema, append-only

| Coluna | Tipo | Descrição |
|---|---|---|
| payload | string | O registro da API, em JSON, exatamente como veio |
| _fonte | string | Nome da fonte |
| _url | string | Endpoint consultado |
| _requisicao | string | Parâmetros/corpo da requisição (JSON) — permite saber o que foi pedido |
| _arquivo_landing | string | Arquivo JSON original no Volume/pasta de landing |
| _id_lote | string | Identificador da execução (`yyyyMMddHHmmss`) |
| _ingerido_em | timestamp | Momento da ingestão |

## Silver

Todas as tabelas têm as colunas de auditoria `_id_lote`, `_ingerido_em` (de qual carga veio a versão atual), `_criado_em` e `_atualizado_em`.

### `silver.producao_industrial` — chave (`atividade_codigo`, `metrica`, `mes_referencia`) · MERGE
| Coluna | Descrição |
|---|---|
| atividade_codigo / atividade_nome | Atividade industrial (ex.: "Metalurgia", "Indústrias de transformação") |
| metrica | `indice` (2022=100) ou `indice_sa` (com ajuste sazonal) |
| mes_referencia | Primeiro dia do mês |
| valor | decimal(14,5) |

### `silver.exportacoes_aco` — chave (`mes_referencia`, `pais`, `sh4_codigo`) · replaceWhere por período
| Coluna | Descrição |
|---|---|
| pais | País de destino |
| sh4_codigo / sh4_descricao | Posição do Sistema Harmonizado (4 dígitos), ex.: 7208 laminados planos |
| valor_fob_usd | Valor FOB em US$ |
| peso_kg | Peso líquido em kg |

### `silver.emissoes_usinas` — chave (`usina_id`, `ano`, `gas`) · MERGE
Identificação, coordenadas (`latitude`, `longitude`), `setor`, `subsetor`, `gas` e `emissoes_t` (toneladas).

### `silver.cambio_dolar` — chave (`data_referencia`) · MERGE
`dolar_ptax` (R$/US$).

### `silver.quarentena` — append
Registros rejeitados de todas as fontes, com o `payload` original e o `motivo_rejeicao`:

| Motivo | Significado |
|---|---|
| `layout_inesperado` | Campos esperados ausentes — a API provavelmente mudou o formato |
| `periodo_invalido` | Data/mês/ano ausente ou fora do formato |
| `valor_indisponivel` | IBGE marcou como não disponível ou sigiloso (`..`, `...`, `-`, `X`) |
| `valor_invalido` | Valor numérico ausente ou não numérico |
| `fora_do_escopo` | País ou subsetor diferente do pedido (filtro ignorado pela API) |
| `data_futura` | Data posterior ao dia da carga |

## Gold

### Dimensões
- **`dim_atividade`** — `atividade_codigo`, `atividade_nome`.
- **`dim_calendario`** — um mês por linha: `mes_referencia`, `ano`, `mes`, `trimestre`, `semestre`, `ano_mes`, `nome_mes`.

### `fato_producao_industrial` — chave (`atividade_codigo`, `mes_referencia`)
| Coluna | Cálculo |
|---|---|
| indice / indice_sa | Índices da PIM-PF |
| var_12m_pct | `indice / indice do mesmo mês no ano anterior − 1` |
| media_movel_3m_sa | Média dos últimos 3 meses do índice dessazonalizado (nula se faltar mês) |

### `fato_exportacao_aco` — chave (`mes_referencia`, `pais`, `sh4_codigo`)
`valor_fob_usd`, `peso_t`, `preco_medio_usd_t` (= FOB / toneladas), `dolar_medio` do mês e `valor_fob_brl` (= FOB × dólar médio).

### `fato_emissoes_usina` — chave (`usina_id`, `ano`)
`emissoes_t_co2e`, `ranking_no_ano` (1 = maior emissora), `participacao_pct` no total do ano, coordenadas.

### `painel_siderurgia_mensal` — chave `mes_referencia`
Tabela larga, pronta para BI:

| Coluna | Descrição |
|---|---|
| indice_metalurgia / indice_metalurgia_sa | Produção da metalurgia (CNAE 24) |
| metalurgia_var_12m_pct | Variação contra o mesmo mês do ano anterior |
| indice_transformacao / indice_industria_geral | Referências para comparação |
| exportacao_t / exportacao_fob_usd / exportacao_fob_brl | Exportações de ferro e aço no mês |
| preco_medio_usd_t | Preço médio por tonelada exportada |
| dolar_medio | PTAX média do mês |
| qtd_paises_destino | Número de países que compraram no mês |
| exportacao_t_var_12m_pct | Variação do volume exportado em 12 meses |
| mes_fechado | `false` para o mês corrente (valores parciais) |

### `painel_emissoes_anual` — chave `ano`
`emissoes_total_t_co2e`, `qtd_usinas`, `maior_emissora`, `participacao_maior_emissora_pct`, `var_ano_anterior_pct`.

> **Nota sobre o índice da PIM-PF:** é um número-índice (2022 = 100), não toneladas. Ele mostra *variação* da produção, não volume absoluto — por isso o projeto não divide emissões por ele para calcular intensidade.
