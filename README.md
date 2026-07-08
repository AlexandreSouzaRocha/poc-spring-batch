# POC — Spring Batch → Kafka (processamento de ~250MM registros)

Processa arquivos do mainframe (particionados pelo dígito verificador da conta, 0–9),
em paralelo com **Spring Batch particionado + virtual threads (Java 21)**, e publica cada
linha no **Kafka**. Metadados/resiliência do batch em **MongoDB (replica set)**. Os
arquivos de entrada vêm do **Azure Blob Storage** (emulado localmente com **Azurite**),
como no cenário real de produção — leitura por byte-range diretamente do blob, sem
baixar o arquivo para disco.

- Layout da linha (**260 bytes**, fixo): `BISD` + `YYYY-MM-DD` + `T23:59:59.9999990000` + `AAAA`(agência, offset 34) + `CCCCCCC`(conta, offset 38, DV = último dígito) + filler de dígitos aleatórios até 260 bytes
- Mensagem Kafka: `{"timestamp": <epoch ms>, "text": "<linha de 260 bytes>"}` com **key `AAAA-CCCCCCC`**
- A aplicação é um **serviço web**; geração e disparo são feitos por **endpoints HTTP**, fire-and-forget
- Roda **shardada em 2 containers** pelo dígito verificador: `app-1` processa `part_0..4.dat` (porta **8081**), `app-2` processa `part_5..9.dat` (porta **8082**)

## Pré-requisitos
- Java 21 (`.tool-versions` → `openjdk-21.0.1`) — só necessário para rodar/testar local, fora de container
- Docker + Docker Compose

## Quickstart (Makefile)
```bash
make up         # sobe kafka, mongo (replica set), kafka-ui, azurite, app-1 e app-2

make generate                 # gera 1MM (100k/dígito), 260 bytes/linha, direto no blob
make trigger                  # dispara o batch nos DOIS containers (fire-and-forget)
make trigger RUN=poc          # dispara com run fixo (permite retomar depois)
make metrics                  # métricas agregadas (job + masterStep) dos dois containers
```
`make help` lista todos os alvos. Acompanhe as mensagens no **kafka-ui**
(http://localhost:8080, tópico `saldo-contas`).

## Endpoints
Collection do Postman em [`docs/`](docs/). Resumo:

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/data/generate?linesPerDigit=100000&recordLength=260` | Gera os 10 arquivos de teste no storage configurado (síncrono; retorna resumo). |
| `POST` | `/batch/trigger[?run=<id>]` | Dispara o batch **fire-and-forget** (responde `202`). Sem `run` = execução nova; com `run` reutilizado = **retomada**. |
| `GET`  | `/actuator/health` | Health da aplicação. |

Geração via script (curl para o endpoint): `./scripts/generate-data.sh [linhas] [recordLength]`.
Para volumes grandes: `make generate LINES=20000000` (200MM total; ~50GB — atenção ao disco).

## Storage: Azure Blob (Azurite) ou disco local
Controlado por `APP_STORAGE` (`blob` nos containers, `file` por padrão localmente):

| Var | Default | Descrição |
|-----|---------|-----------|
| `APP_STORAGE` | `file` | `file` (disco, `APP_INPUT_DIR`) ou `blob` (Azure Blob Storage) |
| `APP_INPUT_DIR` | `./data` | Diretório local (modo `file`) |
| `APP_BLOB_ENDPOINT` | `http://localhost:10000/devstoreaccount1` | Endpoint do blob (modo `blob`) |
| `APP_BLOB_ACCOUNT_NAME` / `APP_BLOB_ACCOUNT_KEY` | credenciais padrão do Azurite | Autenticação Shared Key |
| `APP_BLOB_CONTAINER` | `saldo-files` | Container onde os `part_N.dat` ficam |

A leitura usa `BlobRange` (ranged read) por partição — mesma lógica de byte-offset do
modo disco, sem baixar o blob inteiro. Ver [InputStore](src/main/java/com/bradesco/saldo/batch/storage/InputStore.java),
[BlobStore](src/main/java/com/bradesco/saldo/batch/storage/BlobStore.java) e [LocalFileStore](src/main/java/com/bradesco/saldo/batch/storage/LocalFileStore.java).

> **Nota Azurite:** a chave padrão desta imagem (`mcr.microsoft.com/azure-storage/azurite`)
> é `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`
> — **diferente** da chave "bem conhecida" documentada em versões antigas do Storage
> Emulator. Já configurada corretamente no `docker-compose.yml`/`application.yaml`.

### Inspecionar o blob localmente
Sem instalar nada no host — via Azure CLI em container, já plugado na mesma rede do compose:
```bash
make blob-ls                       # lista os part_N.dat no container saldo-files (nome, tamanho, data)
make blob-cat BLOB=part_0.dat      # baixa e mostra as primeiras linhas de um blob
```
Alternativa com GUI: [Azure Storage Explorer](https://azure.microsoft.com/products/storage/storage-explorer)
→ _Connect to a resource_ → _Storage account_ → _Connection string_, usando
`DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<a de cima>;BlobEndpoint=http://localhost:10000/devstoreaccount1;`
(a porta do Azurite, `10000`, está publicada no host).

## Sharding em 2 containers
| Var | app-1 | app-2 |
|-----|-------|-------|
| `APP_DIGIT_FROM` / `APP_DIGIT_TO` | 0 / 4 | 5 / 9 |

Cada container processa seu subconjunto de arquivos (`part_0..4.dat` / `part_5..9.dat`)
de forma independente. O `JobParameters` inclui um `shard` (identificador `0-4`/`5-9`)
para que os dois containers possam usar o mesmo `run` sem colidir de `JobInstance`.
Disparos simultâneos podem gerar `TransientTransactionError` no Mongo (dois containers
escrevendo metadados ao mesmo tempo); o [BatchLauncherService](src/main/java/com/bradesco/saldo/batch/service/BatchLauncherService.java)
já retenta automaticamente.

**Ganho real:** em um único host, 2 containers competem pela mesma CPU/broker/Mongo — o
ganho é modesto (~10-20%). Sharding só escala de forma quase-linear com hosts distintos.

## Configuração geral (env vars / `application.yaml`)
| Var | Default | Descrição |
|-----|---------|-----------|
| `SERVER_PORT` | `8081` | Porta HTTP da aplicação |
| `APP_CHUNK_SIZE` | `5000` | Linhas por chunk |
| `APP_PARTITIONS_PER_FILE` | `10` | Faixas por arquivo (10 arquivos × N = partições/vthreads) |
| `APP_KAFKA_TOPIC` | `saldo-contas` | Tópico de saída |

## Resiliência (retomar de onde parou)
Dispare com um `run` explícito (`make trigger RUN=poc`), pare o app **graciosamente**
(SIGTERM/`Ctrl+C`) e dispare de novo com o **mesmo `run`**: as partições já `COMPLETED`
são puladas e a que falhou retoma do último chunk commitado (posição salva no Mongo).
Semântica **at-least-once** (pode haver duplicatas do chunk em curso num crash).

> Um `docker kill` (SIGKILL) não dá chance de o Spring Batch marcar a execução como
> `FAILED` — ela fica presa em `STARTED` no Mongo e um novo disparo com o mesmo `run`
> falha com "job execution already running". É uma limitação conhecida do Spring Batch
> com kills abruptos (não é específico do storage em blob); nesse caso, marque a
> execução manualmente como `FAILED` no Mongo antes de redisparar.

Inspecionar metadados:
```bash
docker exec -it poc-mongo mongosh saldo_batch \
  --eval 'db.BATCH_STEP_EXECUTION.find({}, {stepName:1, status:1, writeCount:1}).toArray()'
```

## Monitoria por step
Cada partição (`workerStep`) e o agregado (`masterStep`) logam ao terminar:
```
STEP_METRICS step=workerStep:part_5.dat#7 jobExecutionId=5 status=COMPLETED durationSec=0.69 lidos=500 publicados=500 skips=0 tps=725
STEP_METRICS step=masterStep jobExecutionId=5 status=COMPLETED durationSec=1.84 lidos=50000 publicados=50000 skips=0 tps=27115
```
- `make metrics` → job + masterStep (visão agregada, por container)
- `make metrics-partitions` → uma linha por partição

## Testes
```bash
make test   # cobertura de fronteira do byte-range reader, restart e extração da key
```

## Arquitetura (resumo)
```
POST /batch/trigger  ──(fire-and-forget, virtual thread)──►  JobOperator.start(saldoBatchJob)

saldoBatchJob
  └─ masterStep (Partitioner: arquivos do shard × N faixas por byte-offset)
       └─ TaskExecutorPartitionHandler (virtual threads)
            └─ workerStep (chunk=5000, faultTolerant, restartável)
                 reader    ByteRangeLineReader  (lê a faixa via InputStore, salva offset p/ restart)
                 processor LineProcessor        (offsets fixos → key AAAA-CCCCCCC)
                 writer    KafkaLineWriter       (JSON {timestamp, text}, flush por chunk)

InputStore (abstração de origem dos arquivos)
  ├─ LocalFileStore  (disco, modo dev)
  └─ BlobStore       (Azure Blob Storage / Azurite, ranged reads)
```
