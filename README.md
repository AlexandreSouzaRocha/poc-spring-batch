# POC — Spring Batch → Kafka (processamento de ~50MM registros)

Processa arquivos do mainframe (particionados pelo dígito verificador da conta, 0–9),
em paralelo com **Spring Batch particionado + virtual threads (Java 21)**, e publica cada
linha no **Kafka**. Metadados/resiliência do batch em **MongoDB (replica set)**.

- Layout da linha (**260 bytes**, fixo): `BISD` + `YYYY-MM-DD` + `T23:59:59.9999990000` + `AAAA`(agência, offset 34) + `CCCCCCC`(conta, offset 38, DV = último dígito) + filler de espaços até 260
- Mensagem Kafka: `{"timestamp": <epoch ms>, "text": "<linha de 260 bytes>"}` com **key `AAAA-CCCCCCC`**
- A aplicação é um **serviço web** (porta **8081**); geração e disparo são feitos por **endpoints HTTP**.

## Pré-requisitos
- Java 21 (`.tool-versions` → `openjdk-21.0.1`)
- Docker + Docker Compose

## Quickstart (Makefile)
```bash
make up         # sobe a infra (kafka + mongo replica set + kafka-ui)
make run        # sobe a aplicação em foreground (endpoints em :8081)

# em outro terminal (app no ar):
make generate                 # gera 1MM (100k/dígito), 260 bytes/linha
make trigger                  # dispara o batch (fire-and-forget, execução nova)
make trigger RUN=poc          # dispara com run fixo (permite retomar depois)
```
`make help` lista todos os alvos. Acompanhe as mensagens no **kafka-ui**
(http://localhost:8080, tópico `saldo-contas`).

## Endpoints
Collection do Postman em [`docs/`](docs/). Resumo:

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/data/generate?linesPerDigit=100000&recordLength=260[&dir=&date=]` | Gera os 10 arquivos de teste (síncrono; retorna resumo). |
| `POST` | `/batch/trigger[?run=<id>]` | Dispara o batch **fire-and-forget** (responde `202`). Sem `run` = execução nova; com `run` reutilizado = **retomada**. |
| `GET`  | `/actuator/health` | Health da aplicação. |

Geração via script (curl para o endpoint): `./scripts/generate-data.sh [linhas] [recordLength] [dir]`.
Para 50MM: `make generate LINES=5000000` (arquivos grandes; iterar em volumes menores).

## Configuração (env vars / `application.yaml`)
| Var | Default | Descrição |
|-----|---------|-----------|
| `SERVER_PORT` | `8081` | Porta HTTP da aplicação |
| `APP_INPUT_DIR` | `./data` | Diretório dos arquivos `*.dat` |
| `APP_CHUNK_SIZE` | `5000` | Linhas por chunk |
| `APP_PARTITIONS_PER_FILE` | `8` | Faixas por arquivo (10 arquivos × N = partições/vthreads) |
| `APP_KAFKA_TOPIC` | `saldo-contas` | Tópico de saída |

## Resiliência (retomar de onde parou)
Dispare com um `run` explícito (`make trigger RUN=poc`), mate o app no meio (`Ctrl+C`) e
dispare de novo com o **mesmo `run`**: as partições já `COMPLETED` são puladas e a que
falhou retoma do último chunk commitado (posição salva no Mongo). Semântica
**at-least-once** (pode haver duplicatas do chunk em curso num crash).

Inspecionar metadados:
```bash
docker exec -it poc-mongo mongosh saldo_batch \
  --eval 'db.BATCH_STEP_EXECUTION.find({}, {stepName:1, status:1, writeCount:1}).toArray()'
```

## Testes
```bash
make test   # cobertura de fronteira do byte-range reader, restart e extração da key
```

## Arquitetura (resumo)
```
POST /batch/trigger  ──(fire-and-forget, virtual thread)──►  JobOperator.start(saldoBatchJob)

saldoBatchJob
  └─ masterStep (Partitioner: 10 arquivos × N faixas por byte-offset)
       └─ TaskExecutorPartitionHandler (virtual threads)
            └─ workerStep (chunk=5000, faultTolerant, restartável)
                 reader    ByteRangeLineReader  (lê a faixa, salva offset p/ restart)
                 processor LineProcessor        (offsets fixos → key AAAA-CCCCCCC)
                 writer    KafkaLineWriter       (JSON {timestamp, text}, flush por chunk)
```
