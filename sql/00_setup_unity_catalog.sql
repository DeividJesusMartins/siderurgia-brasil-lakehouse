-- Preparação única do Unity Catalog (rodar por um admin ou alguém com CREATE CATALOG).
-- Os schemas e o volume também são criados automaticamente pelo pipeline
-- (Lakehouse.preparar), mas deixar explícito ajuda a revisar permissões.

CREATE CATALOG IF NOT EXISTS siderurgia_lakehouse_dev;
CREATE CATALOG IF NOT EXISTS siderurgia_lakehouse;

USE CATALOG siderurgia_lakehouse;

CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Respostas brutas das APIs (JSON) + metadados de linhagem';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Dados limpos, tipados e deduplicados por assunto';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Modelo dimensional e painéis para consumo analítico';

-- Volume = pasta governada pelo Unity Catalog. Guarda os JSONs originais das APIs.
CREATE VOLUME IF NOT EXISTS bronze.landing COMMENT 'Arquivos brutos por fonte/lote';

-- Governança: analistas leem só a gold; engenharia escreve em tudo.
-- (ajuste os nomes dos grupos para os do seu workspace)
GRANT USE CATALOG ON CATALOG siderurgia_lakehouse TO `analistas`;
GRANT USE SCHEMA, SELECT ON SCHEMA siderurgia_lakehouse.gold TO `analistas`;
GRANT ALL PRIVILEGES ON CATALOG siderurgia_lakehouse TO `engenharia_dados`;
