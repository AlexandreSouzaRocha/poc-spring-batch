# POC Spring Batch -> Kafka (2 containers shardados pelo dígito verificador)
# Uso típico:
#   make up        # sobe infra + app-1 (dígitos 0-4) + app-2 (dígitos 5-9)
#   make generate  # gera a massa de teste (volume compartilhado ./data)
#   make trigger   # dispara o batch nos DOIS containers (fire-and-forget)
#   make metrics   # métricas agregadas por container (job + masterStep)

# LINES = linhas por dígito (x10 = total) | RECORD = bytes por linha
# RUN   = id de execução; vazio = execução nova. Reutilize p/ retomar de onde parou
APP1_URL ?= http://localhost:8081
APP2_URL ?= http://localhost:8082
LINES    ?= 100000
RECORD   ?= 260
RUN      ?=
MVN      := ./mvnw

.DEFAULT_GOAL := help

.PHONY: help up up-infra down restart logs app-logs metrics metrics-partitions ps build test run generate trigger trigger-1 trigger-2 clean-data clean

help: ## Lista os alvos disponíveis
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## ---------- Stack (infra + 2 apps em container) ----------
up: ## Sobe tudo (kafka, mongo, kafka-ui, app-1 e app-2), buildando a imagem
	docker compose up -d --build
	@echo "kafka-ui: http://localhost:8080  |  app-1 (0-4): $(APP1_URL)  |  app-2 (5-9): $(APP2_URL)"

up-infra: ## Sobe só a infra (sem os apps) — útil p/ rodar o app local com 'make run'
	docker compose up -d kafka kafka-ui mongo mongo-init

down: ## Derruba a stack e remove volumes
	docker compose down -v

restart: down up ## Reinicia a stack do zero

logs: ## Segue os logs de toda a stack
	docker compose logs -f

app-logs: ## Segue os logs dos dois apps
	docker compose logs -f app-1 app-2

metrics: ## Métricas de execução por container (job + masterStep; agregadas)
	docker compose logs app-1 app-2 2>&1 | grep -E "METRICS job=|STEP_METRICS step=masterStep" || echo "nenhuma execução ainda"

metrics-partitions: ## Métricas por partição (workerStep, uma linha por partição)
	docker compose logs app-1 app-2 2>&1 | grep "STEP_METRICS step=workerStep" || echo "nenhuma execução ainda"

ps: ## Status dos containers
	docker compose ps

## ---------- Build/testes locais ----------
build: ## Compila e empacota o boot jar (sem testes)
	$(MVN) -DskipTests package

test: ## Roda os testes de unidade
	$(MVN) test

run: ## Sobe o app local (foreground) contra a infra (use com 'make up-infra')
	$(MVN) spring-boot:run

## ---------- Operação da POC (apps precisam estar no ar) ----------
generate: ## Gera a massa de teste no volume compartilhado (LINES por dígito, RECORD bytes/linha)
	curl -fsS -X POST "$(APP1_URL)/data/generate?linesPerDigit=$(LINES)&recordLength=$(RECORD)"; echo

trigger: ## Dispara o batch nos DOIS containers. Use RUN=<id> para retomar
	curl -fsS -X POST "$(APP1_URL)/batch/trigger$(if $(RUN),?run=$(RUN),)"; echo
	curl -fsS -X POST "$(APP2_URL)/batch/trigger$(if $(RUN),?run=$(RUN),)"; echo

trigger-1: ## Dispara só o app-1 (dígitos 0-4)
	curl -fsS -X POST "$(APP1_URL)/batch/trigger$(if $(RUN),?run=$(RUN),)"; echo

trigger-2: ## Dispara só o app-2 (dígitos 5-9)
	curl -fsS -X POST "$(APP2_URL)/batch/trigger$(if $(RUN),?run=$(RUN),)"; echo

clean-data: ## Remove os arquivos de teste gerados (preserva o diretório do bind mount)
	mkdir -p data && rm -f data/*.dat

clean: clean-data ## Limpa build e dados
	$(MVN) clean
