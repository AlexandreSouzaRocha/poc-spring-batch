# POC Spring Batch -> Kafka
# Uso típico (tudo em container):
#   make up        # sobe infra + app (build da imagem) em background
#   make generate  # gera a massa de teste (app no ar)
#   make trigger   # dispara o batch (fire-and-forget)
#   make metrics   # métricas de execução a partir do log do app

# LINES = linhas por dígito (x10 = total) | RECORD = bytes por linha
# RUN   = id de execução; vazio = execução nova. Reutilize p/ retomar de onde parou
APP_URL ?= http://localhost:8081
LINES   ?= 100000
RECORD  ?= 260
RUN     ?=
MVN     := ./mvnw

.DEFAULT_GOAL := help

.PHONY: help up up-infra down restart logs app-logs metrics ps build test run generate trigger clean-data clean

help: ## Lista os alvos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

## ---------- Stack (infra + app em container) ----------
up: ## Sobe tudo (kafka, mongo, kafka-ui e o app) em background, buildando a imagem
	docker compose up -d --build
	@echo "kafka-ui: http://localhost:8080  |  app: $(APP_URL)"

up-infra: ## Sobe só a infra (sem o app) — útil p/ rodar o app local com 'make run'
	docker compose up -d kafka kafka-ui mongo mongo-init

down: ## Derruba a stack e remove volumes
	docker compose down -v

restart: down up ## Reinicia a stack do zero

logs: ## Segue os logs de toda a stack
	docker compose logs -f

app-logs: ## Segue os logs do app
	docker compose logs -f app

metrics: ## Métricas de execução (linhas METRICS do log do app)
	docker compose logs app 2>&1 | grep "METRICS" || echo "nenhuma execução ainda"

ps: ## Status dos containers
	docker compose ps

## ---------- Build/testes locais ----------
build: ## Compila e empacota o boot jar (sem testes)
	$(MVN) -DskipTests package

test: ## Roda os testes de unidade
	$(MVN) test

run: ## Sobe o app local (foreground) contra a infra (use com 'make up-infra')
	$(MVN) spring-boot:run

## ---------- Operação da POC (app precisa estar no ar) ----------
generate: ## Gera a massa de teste (LINES por dígito, RECORD bytes/linha)
	curl -fsS -X POST "$(APP_URL)/data/generate?linesPerDigit=$(LINES)&recordLength=$(RECORD)"; echo

trigger: ## Dispara o batch (fire-and-forget). Use RUN=<id> para retomar
	curl -fsS -X POST "$(APP_URL)/batch/trigger$(if $(RUN),?run=$(RUN),)"; echo

clean-data: ## Remove os arquivos de teste gerados
	rm -rf data

clean: clean-data ## Limpa build e dados
	$(MVN) clean
