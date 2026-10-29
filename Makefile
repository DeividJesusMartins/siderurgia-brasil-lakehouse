.PHONY: help install lint format test test-delta run run-full fonte gold sql bundle-validate bundle-deploy clean

help: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Instala o projeto com Spark, Delta e ferramentas de dev
	pip install -e ".[dev,spark]"
	pre-commit install || true

lint: ## Verifica estilo e erros comuns
	ruff check .
	ruff format --check .

format: ## Formata o código
	ruff check . --fix
	ruff format .

test: ## Testes unitários e de fluxo (sem Delta, rápidos)
	pytest --cov --cov-report=term-missing

test-delta: ## Testes de integração com Delta Lake (baixa os JARs na 1ª vez)
	pytest -m delta

run: ## Executa o pipeline completo (incremental)
	python -m siderurgia_lakehouse --camada todas

run-full: ## Reprocessa todo o histórico desde DATA_INICIO
	python -m siderurgia_lakehouse --camada todas --full-refresh

fonte: ## Roda bronze+silver de uma fonte: make fonte F=sidra_pimpf
	python -m siderurgia_lakehouse --camada bronze --fontes $(F)
	python -m siderurgia_lakehouse --camada silver --fontes $(F)

gold: ## Recalcula só a camada gold
	python -m siderurgia_lakehouse --camada gold

sql: ## Abre um shell spark-sql com Delta para consultar as tabelas locais
	pyspark --packages io.delta:delta-spark_2.12:3.2.1 \
	  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

bundle-validate: ## Valida o Databricks Asset Bundle
	databricks bundle validate

bundle-deploy: ## Publica no Databricks (target dev)
	databricks bundle deploy -t dev

clean: ## Remove dados locais e artefatos de build
	rm -rf lakehouse/ spark-warehouse/ metastore_db/ derby.log dist/ build/ .pytest_cache .ruff_cache .coverage
