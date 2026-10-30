<p align="center"><img src="img/logo_siderurgia_brasil.png" width="420" alt="Siderurgia Brasil Lakehouse"></p>

# Dashboard Power BI

Relatório em formato **PBIP** (Power BI Project), versionado no Git:
- **modelo semântico** em **TMDL**, com tabelas, relacionamentos e medidas DAX em texto;
- **relatório** em **PBIR**, com um JSON por página e por visual.

Ele lê as tabelas da camada **gold** exportadas em Parquet.

```
powerbi/
├── SiderurgiaBrasil.pbip                 ← abra este arquivo no Power BI Desktop
├── SiderurgiaBrasil.SemanticModel/       ← modelo (TMDL)
│   └── definition/
│       ├── expressions.tmdl              ← parâmetro PastaDados
│       ├── relationships.tmdl
│       └── tables/                       ← 7 tabelas da gold, _Medidas, tradução e parâmetros de campo
└── SiderurgiaBrasil.Report/              ← relatório (PBIR)
    ├── definition/pages/                 ← 4 páginas
    └── StaticResources/RegisteredResources/  ← tema e logo
```

## Como abrir

1. **Gere os arquivos da gold**, depois de rodar o pipeline:
   ```bash
   python scripts/exportar_powerbi.py
   ```
   O script cria a pasta `exports/` com 7 arquivos `.parquet` e o `gold_powerbi.zip`.
2. **Coloque os arquivos `.parquet` numa pasta do seu computador.** O padrão é `C:\Estudos\gold_powerbi\`.
3. **Abra o `powerbi/SiderurgiaBrasil.pbip`** no Power BI Desktop.
4. Se a pasta for outra, ajuste o caminho em **Transformar dados → Gerenciar parâmetros → `PastaDados`**. O caminho precisa terminar com `\`.
5. Clique em **Atualizar**. O PBIP não guarda dados, só a definição do relatório.

## Páginas

| Página | Perguntas que responde | Destaques |
|---|---|---|
| **Visão Geral** | Como estão produção, exportações, preço e emissões agora? | 4 KPIs com variação; produção da metalurgia × transformação; exportações × dólar; destinos, produtos e maiores emissoras |
| **Produção** | A metalurgia acompanha o restante da indústria? Que atividades crescem? | Séries dessazonalizadas, média móvel de 3 meses, ranking de variação em 12 meses por atividade |
| **Exportações** | O câmbio puxa as exportações? Para onde e o quê o Brasil exporta? | Volume, receita e preço em 12 meses com variação; concentração dos 5 maiores destinos; top 10 destinos e produtos (SH4) |
| **Emissões** | Quanto o setor emite e quão concentradas estão as emissões? | Total do setor com variação anual; participação das 3 maiores usinas; ranking por usina |

## Navegação, idioma e filtros

- **Navegação:** botões em formato de pílula no cabeçalho; a página atual fica destacada em laranja.
- **Idioma:** seletor **pt / en / es** à direita da linha de filtros, sincronizado entre as páginas.
- **Filtros** de cada página, logo abaixo do cabeçalho:

| Página | Filtros |
|---|---|
| Visão Geral | Ano · País de destino · Usina |
| Produção | Ano · Mês · Atividade industrial |
| Exportações | Ano · País de destino · Produto (SH4) |
| Emissões | Ano · Usina |

Os rankings, os top 10 e as participações (%) se recalculam sobre a seleção. Os KPIs de 12 meses terminam no último mês com dado dentro do filtro.

## Tradução (pt / en / es)

O relatório troca de idioma sem duplicar páginas:

| Peça do modelo | Papel |
|---|---|
| `Tradução Idiomas` | Tabela calculada (`DATATABLE`) com `IdiomaID`, nome do idioma e cultura (`pt-BR`, `en-US`, `es-ES`). É a origem do seletor. |
| `Tradução Textos` | Tabela em Power Query com **texto · idioma · chave**. Todos os rótulos do relatório vêm daqui. |
| Medidas `Tradução - <chave>` | Devolvem o texto da chave no idioma selecionado (padrão `pt`). Ficam na pasta *9 Traduções*. |
| `Parâmetro - …` | Parâmetros de campo com o rótulo traduzido de cada série/coluna, relacionados a `Tradução Idiomas`. Traduzem legendas, cabeçalhos de tabela e o filtro de Mês. |

- Títulos de página, KPIs, gráficos, filtros e botões usam **expressão condicional (fx)** apontando para as medidas `Tradução - …`.
- As frases dinâmicas (▲/▼, "vs 12 meses anteriores", "12 meses até out/2026") montam o texto com essas medidas, e datas e percentuais usam `FORMAT(..., [Tradução - Cultura])` para seguir o idioma.
- **Para incluir ou corrigir um texto:** edite a tabela `Tradução Textos` no Power Query e use a medida `Tradução - <chave>`.
- Nomes que vêm das fontes (países, produtos, atividades do IBGE, usinas) continuam como publicados.

## Decisões de modelagem

- **Esquema estrela:** `dim_calendario` (mensal) e `dim_atividade` filtram os fatos. As medidas ficam todas na tabela `_Medidas`, organizadas em pastas.
- **KPIs "últimos 12 meses":** a janela termina no **último mês com dado** dentro do filtro, e não no mês atual. Assim, a defasagem de publicação das fontes não distorce a comparação.
- **Variações em texto** (▲/▼) para leitura rápida nos cartões, com a data de referência logo abaixo.
- **Top N por medida:** o `RANKX` fica dentro da medida, e não no filtro do visual. Isso mantém o JSON do relatório simples e a regra visível no modelo.
- **Índices dessazonalizados** nas comparações, para que a sazonalidade não pareça tendência.
- **Ranking de usinas:** 🏆 marca as 5 maiores emissoras e ⚠️ as 5 menores entre as usinas filtradas (medida `Destaque Ranking (ícone)`).
- **Emissões:** estimativas independentes do Climate TRACE, apresentadas de forma factual, sem juízo sobre o desempenho de nenhuma empresa.

## Identidade visual

- **Paleta:** grafite do aço (`#1F2933`), laranja do aço incandescente (`#E8590C`), azul-aço (`#3B5B7A`) e âmbar (`#F59F00`), definida no tema `SiderurgiaBrasil.json`.
- **Logo:** original do projeto (perfil de viga "I"), em `docs/img/`.
