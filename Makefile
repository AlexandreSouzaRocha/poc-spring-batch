# POC Spring Batch -> Kafka (processamento orientado a evento)
# Fluxo: blob criado -> evento no tópico saldo-file-processor (simula Event Grid+Event
# Hub) -> 10 containers (app-1..app-10) consomem, 1 partição cada (consumer group,
# rebalance automático) -> processa e SÓ FAZ ACK após concluir.
# Uso típico:
#   make up        # sobe infra (kafka, mongo, azurite) + os 10 containers da app
#   make generate  # gera a massa de teste no blob -> dispara o processamento sozinho
#   make consumer-group  # ver a atribuição de partições (1 por container) e o lag
#   make metrics   # métricas agregadas por container (job + fileMasterStep)
# make trigger-file reprocessa manualmente um arquivo específico (ex.: após corrigir
# um arquivo que caiu no DLQ) — o fluxo normal é 100% automático via evento.

# LINES = linhas por dígito (x10 = total) | RECORD = bytes por linha
APP1_URL      ?= http://localhost:8081
APPS          := app-1 app-2 app-3 app-4 app-5 app-6 app-7 app-8 app-9 app-10
LINES         ?= 100000
RECORD        ?= 260
FILE          ?=
BLOB_ACCOUNT  := devstoreaccount1
BLOB_KEY      := Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
BLOB_ENDPOINT := http://azurite:10000/devstoreaccount1
BLOB_CONTAINER:= saldo-files
AZ_CLI        := docker run --rm --network poc-spring-batch_default mcr.microsoft.com/azure-cli:latest az
MVN           := ./mvnw

.DEFAULT_GOAL := help

.PHONY: help up up-infra down restart logs app-logs metrics metrics-partitions consumer-group dlq ps build test run generate trigger-file blob-ls blob-cat clean

help: ## Lista os alvos disponíveis
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## ---------- Stack (infra + 10 apps em container, 1 por partição) ----------
up: ## Sobe tudo (kafka, mongo, kafka-ui, azurite, 10 containers da app), buildando a imagem
	docker compose up -d --build
	@echo "kafka-ui: http://localhost:8080  |  apps: :8081..:8090 (app-1..app-10)"

up-infra: ## Sobe só a infra (sem os apps) — útil p/ rodar o app local com 'make run'
	docker compose up -d kafka kafka-ui mongo mongo-init azurite

down: ## Derruba a stack e remove volumes
	docker compose down -v

restart: down up ## Reinicia a stack do zero

logs: ## Segue os logs de toda a stack
	docker compose logs -f

app-logs: ## Segue os logs dos 10 containers da app
	docker compose logs -f $(APPS)

metrics: ## Métricas de execução por container (job + fileMasterStep; agregadas)
	docker compose logs $(APPS) 2>&1 | grep -E "METRICS job=|STEP_METRICS step=fileMasterStep" || echo "nenhuma execução ainda"

metrics-partitions: ## Métricas por partição (workerStep, uma linha por partição)
	docker compose logs $(APPS) 2>&1 | grep "STEP_METRICS step=workerStep" || echo "nenhuma execução ainda"

consumer-group: ## Atribuição de partições e lag do consumer group saldo-file-processor-group
	docker exec poc-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group saldo-file-processor-group

dlq: ## Mostra mensagens no dead-letter topic (arquivos que excederam as tentativas)
	docker exec poc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic saldo-file-processor.dlq --from-beginning --timeout-ms 5000

ps: ## Status dos containers
	docker compose ps

## ---------- Build/testes locais ----------
build: ## Compila e empacota o boot jar (sem testes)
	$(MVN) -DskipTests package

test: ## Roda os testes de unidade
	$(MVN) test

run: ## Sobe o app local (foreground) contra a infra (use com 'make up-infra')
	$(MVN) spring-boot:run

## ---------- Operação da POC ----------
generate: ## Gera a massa de teste no Azure Blob (dispara o processamento automaticamente)
	curl -fsS -X POST "$(APP1_URL)/data/generate?linesPerDigit=$(LINES)&recordLength=$(RECORD)"; echo

trigger-file: ## [manual/ad-hoc] Reprocessa um arquivo específico. Use FILE=<timestamp>_part_N.dat
	curl -fsS -X POST "$(APP1_URL)/batch/trigger-file?file=$(FILE)"; echo

## ---------- Inspecionar o Azure Blob (Azurite) localmente ----------
blob-ls: ## Lista os blobs no container saldo-files (via Azure CLI em container)
	$(AZ_CLI) storage blob list \
		--account-name $(BLOB_ACCOUNT) --account-key "$(BLOB_KEY)" \
		--blob-endpoint "$(BLOB_ENDPOINT)" --container-name $(BLOB_CONTAINER) \
		--output table

blob-cat: ## Baixa e mostra as primeiras linhas de um blob. Use BLOB=<timestamp>_part_0.dat
	docker run --rm --network poc-spring-batch_default -v /tmp:/out mcr.microsoft.com/azure-cli:latest \
		az storage blob download \
			--account-name $(BLOB_ACCOUNT) --account-key "$(BLOB_KEY)" \
			--blob-endpoint "$(BLOB_ENDPOINT)" --container-name $(BLOB_CONTAINER) \
			--name $(BLOB) --file /out/$(BLOB) --no-progress -o none
	head -c 2000 /tmp/$(BLOB); echo

clean: ## Limpa build local (os arquivos ficam no blob; 'make down' remove o volume do Azurite)
	$(MVN) clean
