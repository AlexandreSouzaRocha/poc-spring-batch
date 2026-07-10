# POC — Spring Batch orientado a evento (Blob → Kafka) → Kafka

Processa arquivos do mainframe (particionados pelo dígito verificador da conta, 0–9),
em paralelo com **Spring Batch particionado + virtual threads (Java 21)**, e publica cada
linha no **Kafka**. Arquivos vêm do **Azure Blob Storage** (Azurite localmente) — leitura
por byte-range direto do blob, sem baixar pra disco. Metadados/resiliência do batch em
**MongoDB (replica set)**.

**Fluxo primário — orientado a evento (produção-like):**
```
Blob criado ──► Event Grid ──► Event Hub ──► tópico Kafka "saldo-file-processor"
                                              (10 partições = 1 por dígito verificador)
                                                   │
                          consumer group (Kafka rebalanceia entre containers)
                                                   ▼
                          processa o arquivo (síncrono) ──► ACK só após concluir
```
Localmente, como o Azurite não emula Event Grid, a própria aplicação publica o evento
no tópico assim que termina de subir cada arquivo (simula o que Event Grid+Event Hub
entregariam). Em Azure real essa ligação é **infraestrutura** (Event Grid System Topic
na Storage Account, assinatura filtrando `BlobCreated`, destino = Event Hub) — nenhuma
mudança de código, já que **Event Hub expõe um endpoint Kafka nativo**: o mesmo
consumer aponta para lá só trocando `bootstrap-servers`/SASL.

- Layout da linha (**260 bytes**, fixo): `BISD` + `YYYY-MM-DD` + `T23:59:59.9999990000` + `AAAA`(agência, offset 34) + `CCCCCCC`(conta, offset 38, DV = último dígito) + filler de dígitos aleatórios até 260 bytes
- Nome do arquivo: `<timestamp>_part_<dígito>.dat` (o timestamp evita colisão entre gerações diferentes do mesmo dígito — como um mainframe reenviando o arquivo todo dia)
- Mensagem Kafka de saída: `{"timestamp": <epoch ms>, "text": "<linha de 260 bytes>"}` com **key `AAAA-CCCCCCC`**

📊 **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — diagramas (topologia do sistema e
pipeline de processamento por arquivo) com os detalhes de resiliência de cada etapa.

## Pré-requisitos
- Java 25 (`.tool-versions` → `openjdk-25.0.2`) — só necessário para rodar/testar local, fora de container
- Docker + Docker Compose

## Quickstart (Makefile)
```bash
make up         # sobe kafka, mongo (replica set), kafka-ui, azurite e 10 containers da app

make generate                 # gera 1MM (100k/dígito), 260 bytes/linha, direto no blob
                               # -> publica os eventos -> processamento começa sozinho
make consumer-group           # ver o rebalanceamento/lag por partição
make metrics                  # métricas agregadas (job + fileMasterStep) de todos os containers
```
`make help` lista todos os alvos. Acompanhe as mensagens no **kafka-ui**
(http://localhost:8080, tópico `saldo-contas`).

## Endpoints
Collection do Postman em [`docs/`](docs/). Resumo:

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/data/generate?linesPerDigit=100000&recordLength=260` | Gera os 10 arquivos de teste no blob **e dispara o processamento via evento** (síncrono; retorna resumo). |
| `POST` | `/batch/trigger-file?file=<nome>` | **[Manual/ad-hoc]** reprocessa um único arquivo (mesmo job usado pelo consumer). |
| `GET`  | `/actuator/health` | Health da aplicação. |

O fluxo automático é via evento (Kafka) — `/batch/trigger-file` existe só como
**reprocessamento manual/operacional** (ex.: depois de corrigir um arquivo que caiu no DLQ),
não é necessário no dia a dia.

Para volumes grandes: `make generate LINES=20000000` (200MM total; ~50GB — atenção ao disco).

## Storage: Azure Blob (Azurite) ou disco local
Controlado por `APP_STORAGE` (`blob` nos containers, `file` por padrão localmente):

| Var | Default | Descrição |
|-----|---------|-----------|
| `APP_STORAGE` | `file` | `file` (disco, `APP_INPUT_DIR`) ou `blob` (Azure Blob Storage) |
| `APP_INPUT_DIR` | `./data` | Diretório local (modo `file`) |
| `APP_BLOB_ENDPOINT` | `http://localhost:10000/devstoreaccount1` | Endpoint do blob (modo `blob`) |
| `APP_BLOB_ACCOUNT_NAME` / `APP_BLOB_ACCOUNT_KEY` | credenciais padrão do Azurite | Autenticação Shared Key |
| `APP_BLOB_CONTAINER` | `saldo-files` | Container onde os arquivos ficam |

Ver [InputStore](src/main/java/com/bradesco/saldo/batch/storage/InputStore.java),
[BlobStore](src/main/java/com/bradesco/saldo/batch/storage/BlobStore.java) e [LocalFileStore](src/main/java/com/bradesco/saldo/batch/storage/LocalFileStore.java).

> **Nota Azurite:** a chave padrão desta imagem (`mcr.microsoft.com/azure-storage/azurite`)
> é `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`
> — **diferente** da chave "bem conhecida" documentada em versões antigas do Storage
> Emulator. Já configurada corretamente no `docker-compose.yml`/`application.yaml`.

### Inspecionar o blob localmente
```bash
make blob-ls                                    # lista os arquivos no container saldo-files
make blob-cat BLOB=1720471234567_part_0.dat     # baixa e mostra as primeiras linhas
```
Alternativa com GUI: [Azure Storage Explorer](https://azure.microsoft.com/products/storage/storage-explorer)
→ _Connect to a resource_ → _Storage account_ → _Connection string_, usando
`DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<a de cima>;BlobEndpoint=http://localhost:10000/devstoreaccount1;`

## Sharding elástico via consumer group Kafka
O tópico `saldo-file-processor` tem **10 partições** (1 por dígito verificador). Os **10
containers** (`app-1`..`app-10`) consomem no **mesmo consumer group**
(`saldo-file-processor-group`) — o Kafka distribui **exatamente 1 partição para cada
um** automaticamente (validado via `make consumer-group`: 10 hosts distintos, 1
partição cada). Se um container cair, o Kafka reatribui a partição órfã aos
sobreviventes até ele voltar — elástico, sem precisar reconfigurar nada (ao contrário do
sharding estático por env var usado antes de existir o consumer group).

`group.instance.id` é fixado por container (via hostname) para membership estático:
evita rebalance desnecessário em restarts rápidos. `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=5000`
dá tempo dos 10 containers entrarem no grupo antes do primeiro rebalance, evitando uma
cascata de reassignments (1 por container que sobe) no cold start.

**Ganho real:** em um único host, os containers competem pela mesma CPU/broker/Mongo — o
ganho de mais containers é modesto. Sharding escala de forma quase-linear com hosts distintos.

> **Corrida rara no cold start — nunca perde o arquivo:** o DAO de `JobInstance` do
> Spring Batch faz um check-then-act sem lock atômico — em um cold start com 10
> containers entrando no grupo ao mesmo tempo, dois consumers podem, raramente, tentar
> criar a mesma `JobInstance` simultaneamente, deixando uma órfã (sem execução
> associada). Como isso é dado de saldo, o consumer **não desiste do arquivo**: escala
> automaticamente para uma identidade nova (`variant=1`, `2`...) até achar uma que
> funcione — o DLQ fica reservado só para falha real de processamento do conteúdo,
> nunca para esse tipo de corrida de metadado. Ver
> [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#corrida-rara-no-cold-start--nunca-perde-o-arquivo).

## Configuração geral (env vars / `application.yaml`)
| Var | Default | Descrição |
|-----|---------|-----------|
| `SERVER_PORT` | `8081` | Porta HTTP da aplicação |
| `APP_CHUNK_SIZE` | `5000` | Linhas por chunk |
| `APP_PARTITIONS_PER_FILE` | `10` | Faixas por arquivo (byte-offset) processadas em paralelo por virtual threads. Em benchmark com 1 CPU/container, `8` rendeu melhor resultado que `10` — ver [seção de benchmark](#benchmark-de-performance) |
| `APP_KAFKA_TOPIC` | `saldo-contas` | Tópico de saída |
| `APP_FILE_PROCESSOR_TOPIC` | `saldo-file-processor` | Tópico de entrada (evento de arquivo pronto) |
| `APP_FILE_PROCESSOR_DLQ_TOPIC` | `saldo-file-processor.dlq` | Dead-letter: arquivos que excederam as tentativas |
| `APP_FILE_PROCESSOR_GROUP_ID` | `saldo-file-processor-group` | Consumer group (compartilhado entre containers) |
| `APP_FILE_PROCESSOR_MAX_ATTEMPTS` | `6` | Tentativas antes de mandar pro DLQ + pasta de erros (aumentado de 3→6 após observar DLQ falso-positivo em cold start sob alta concorrência — ver [benchmark](#benchmark-de-performance)) |
| `APP_FILE_PROCESSOR_RETRY_BACKOFF_SECONDS` | `10` | Espera entre tentativas (nack) |

## Resiliência
**Processamento síncrono com ACK manual**: o consumer (`enable-auto-commit: false`,
`ack-mode: manual_immediate`) só confirma a mensagem no Kafka **depois** do job do
arquivo terminar com `COMPLETED`. Se o job falhar, `Acknowledgment.nack(backoff)` faz o
Kafka redeliverar a mesma mensagem depois de um tempo — sem perder o arquivo.

**Retomar de onde parou:** os `JobParameters` (nome do arquivo, que já é único por
timestamp) são a identidade determinística do `JobInstance`. Se o processo morrer no
meio (crash, OOM-kill, `docker kill`), a mensagem nunca foi confirmada — na redelivery
(mesmo container reiniciando, ou outro após rebalance), o
[FileProcessorConsumer](src/main/java/com/bradesco/saldo/batch/consumer/FileProcessorConsumer.java)
detecta a execução anterior presa em `STARTED` (só pode ser órfã — o Kafka garante um
único consumer por partição) e a marca como `FAILED`, liberando o Spring Batch para
**retomar as partições incompletas** em vez de duplicar. Validado matando o container
(`docker kill`) no meio de um arquivo de 20MM linhas: retomou e completou certinho.

**Redelivery de trabalho já concluído:** se a mesma mensagem chegar de novo depois do
arquivo já ter sido processado com sucesso (at-least-once), o Spring Batch recusa rodar
de novo (`JobInstanceAlreadyCompleteException`) — tratado como sucesso idempotente
(confirma sem reprocessar), em vez de ficar retentando pra sempre e travando a partição.

**Poison message (arquivo com falha persistente):** após `APP_FILE_PROCESSOR_MAX_ATTEMPTS`
tentativas falhas, o arquivo é movido para `errors/` no blob e a mensagem original vai
pro tópico `.dlq` — a partição **não trava**, segue processando os próximos arquivos.
Validado publicando uma mensagem apontando pra um arquivo inexistente: 3 tentativas,
depois DLQ, lag da partição voltou a zero.

> `docker kill` (SIGKILL) não dá chance de marcar a execução como `FAILED` no momento —
> por isso a recuperação acontece na **próxima mensagem** para aquele arquivo (redelivery
> do Kafka), não instantaneamente no restart do container. É esperado.

Inspecionar metadados:
```bash
docker exec -it poc-mongo mongosh saldo_batch \
  --eval 'db.batch_step_execution.find({}, {name:1, status:1, write_count:1}).toArray()'
make consumer-group   # partições, offsets e lag do consumer group
make dlq               # mensagens no dead-letter topic
```

## Monitoria por step
Cada partição (`workerStep`) e o agregado (`fileMasterStep`) logam ao terminar:
```
STEP_METRICS step=workerStep:1720471234567_part_5.dat#7 jobExecutionId=5 status=COMPLETED durationSec=0.69 lidos=500 publicados=500 skips=0 tps=725
STEP_METRICS step=fileMasterStep jobExecutionId=5 status=COMPLETED durationSec=1.84 lidos=50000 publicados=50000 skips=0 tps=27115
```
- `make metrics` → job + step agregado (visão por container)
- `make metrics-partitions` → uma linha por partição

## Benchmark de Performance

> ⚠️ **Limitações da máquina de teste — leia antes de interpretar os números abaixo.**
> Todos os testes foram rodados **localmente**, em **Docker Desktop (macOS) com 12
> vCPUs / 36GiB RAM** alocados à VM, competindo no mesmo host físico por CPU entre os
> 10 containers da aplicação, Kafka, MongoDB e Azurite. Isso é **fundamentalmente
> diferente** do ambiente-alvo de produção (Kubernetes via ArgoCD), onde Kafka, MongoDB
> e Azure Blob Storage real rodam em infraestrutura **separada** dos pods de
> processamento — sem disputar CPU com eles. Três artefatos específicos deste
> laboratório local, que **não devem se repetir** (ou devem ser bem menores) em produção:
> - **Azurite** (emulador local do Azure Blob Storage) é um processo Node.js único,
>   com metadados persistidos em arquivo JSON — não escala para múltiplos GB com
>   escritas/leituras concorrentes como o Azure Blob Storage real. Foi o gargalo
>   dominante na fase de **geração** dos arquivos de teste (não afeta o tempo de
>   *processamento*, que é a métrica que importa para a SLA de produção).
> - O broker **Kafka (KRaft, single-node)** ocasionalmente apresenta instabilidade de
>   coordenador logo após subir (`This is not the correct coordinator`) — observado
>   tanto com 10 quanto com 3 containers consumindo; não impediu nenhum teste de
>   completar, mas adiciona alguns segundos de latência de rebalance. Em produção, um
>   cluster Kafka multi-broker gerenciado não deve apresentar esse comportamento.
> - **MongoDB** usa um único documento contador compartilhado (`batch_sequences`) para
>   gerar IDs sequenciais — sob alta concorrência de cold-start (10 containers criando
>   `StepExecution`s simultaneamente), gera conflitos de escrita transitórios
>   (`NoSuchTransaction`), mitigados com retry exponencial (`CustomSequenceIncrementer`).
>
> Os números abaixo são **direcionais** (mostram tendência de escala e viabilidade da
> arquitetura), não uma medição absoluta da capacidade em produção.

### Cenário 1 — 10 containers (topologia real: 1 arquivo por dígito verificador)

Cada container com **1 CPU / 2GiB memória** (limit), `partitions-per-file=8`,
`chunk-size=5000`, Java 25 (virtual threads, sem pinning — JEP 491), Kafka/MongoDB com
CPU dedicada separada dos containers de app. Volume dividido igualmente entre os 10
arquivos (ex.: 30MM = 3MM linhas/arquivo).

| Volume total | Tempo de processamento | Throughput agregado | Resultado |
|---:|---:|---:|:--|
| 1MM | 21.1s | ~47.400 registros/s | 10/10, 0 erros |
| 5MM | 37.2s | ~134.500 registros/s | 10/10, 0 erros |
| 10MM | 61.0s | ~164.000 registros/s | 10/10, 0 erros |
| 20MM | 88.7s | ~225.600 registros/s | 10/10, 0 erros |
| 30MM | 103.4s | ~290.200 registros/s | 10/10, 0 erros |

Throughput cresce com o volume porque custos fixos (restart dos containers,
rebalance do consumer group, warm-up) são amortizados sobre mais dados — o comportamento
esperado de um pipeline com overhead de inicialização fixo.

Reproduzir: `./scripts/benchmark.sh <totalRecords> [recordLength] [timeoutSeconds]`.

### Cenário 2 — Teste de capacidade por container (mais fiel a produção)

Rodar os 10 containers simultaneamente neste laboratório de 12 núcleos mistura dois
efeitos: a capacidade real de processamento de cada container, e a contenção de CPU
local entre app/Kafka/Mongo/Azurite — que **não existirá em produção** (infra separada
dos pods no Kubernetes). Para isolar a métrica que importa (capacidade de
processamento por container/pod), este cenário roda só **3 containers**, cada um com
**3 CPUs dedicadas**, processando **1 arquivo no tamanho real de produção** cada — o
restante da máquina (9 núcleos) fica livre para Kafka/MongoDB/Azurite, eliminando a
disputa de recursos que distorce o Cenário 1.

Volumes usados: 20MM linhas/arquivo (carga inicial de produção, 200MM ÷ 10 arquivos) e
6MM linhas/arquivo (carga de homologação, 60MM ÷ 10 arquivos).

> ⚠️ **Bug de metodologia encontrado e corrigido:** o tópico `saldo-file-processor` tem
> 10 partições fixas (1 por dígito), mas este cenário só ativa 3 containers/dígitos
> (0, 1, 2). O assignor padrão do Kafka (`RangeAssignor`) distribui partições em
> **faixas contíguas** — com 3 consumers para 10 partições, a faixa `[0-3]` (onde caem
> os 3 dígitos com tráfego real) podia inteira parar num **único container**, deixando
> os outros 2 ociosos e o "throughput/container" reportado (agregado ÷ 3) sendo uma
> divisão fictícia, não uma medição real de 3 containers trabalhando em paralelo.
> Corrigido trocando para `partition.assignment.strategy: RoundRobinAssignor`
> (distribui ciclicamente partição a partição, garantindo 1 dígito real por container
> ativo — confirmado via log `partitions assigned` antes de aceitar o resultado). Os
> números do teste de **produção** abaixo já refletem a correção (também rodado com
> `chunk-size=10000`, não 5000); o de **homologação** ainda é o valor antigo
> (`chunk-size=5000`) e não foi revalidado — trate com cautela até re-executar.

| Cenário | Linhas/arquivo | Total (3 containers) | chunk-size | Tempo de processamento | Throughput/container | Resultado |
|---|---:|---:|---:|---:|---:|:--|
| Homologação (não revalidado) | 6MM | 18MM | 5000 | 61.1s | ~98.360 registros/s | 3/3, 0 erros |
| Produção (revalidado, distribuição confirmada) | 20MM | 60MM | 10000 | 135.9s | ~148.148 registros/s | 3/3, 0 erros |

**Extrapolação para os 10 containers de produção** (assumindo throughput/container
estável e infra dedicada, sem a contenção local que documentamos acima):
- Produção (200MM, 20MM/arquivo): ~148.148 × 10 ≈ 1.481.480 registros/s agregado → 200MM em ~135s (**dentro da meta de 5 minutos**).

Essa extrapolação é linear e otimista (assume que Kafka/Mongo/Blob real não viram
gargalo em produção, o que é razoável dado que são serviços gerenciados/dedicados,
mas **não foi validado empiricamente** — só um laboratório com hosts físicos
separados, ou o próprio cluster Kubernetes alvo, pode confirmar).

Reproduzir: `./scripts/capacity-test.sh <linesPerFile> [recordLength] [timeoutSeconds] [coresPerContainer]`.
Após rodar, sempre conferir `docker compose logs app-1 app-2 app-3 | grep "partitions assigned"`
para confirmar que os dígitos ativos caíram em containers diferentes antes de aceitar o resultado.

## Testes
```bash
make test   # fronteira do byte-range reader, restart, extração da key, naming, partitioner, DLQ
```

## Arquitetura (resumo)
Diagramas completos (topologia + pipeline, com badges de resiliência por etapa): [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
```
AccountFileGenerator (POST /data/generate)
  └─ grava no InputStore (blob ou disco) e publica evento no tópico
     saldo-file-processor (partição explícita = dígito)

FileProcessorConsumer (@KafkaListener, ack manual)
  └─ 1 mensagem = 1 arquivo. Roda saldoFileJob (síncrono) e só faz ack no COMPLETED.
     JobInstance identificado pelo nome do arquivo -> redelivery resume/idempotente.
     Falha persistente (N tentativas) -> errors/ + tópico .dlq, sem travar a partição.

saldoFileJob
  └─ fileMasterStep (1 arquivo, particionado por byte-range)
       └─ TaskExecutorPartitionHandler (virtual threads)
            └─ workerStep (chunk=5000, faultTolerant, restartável)
                 reader    ByteRangeLineReader  (lê a faixa via InputStore, salva offset p/ restart)
                 processor LineProcessor        (offsets fixos → key AAAA-CCCCCCC)
                 writer    KafkaLineWriter       (JSON {timestamp, text}, flush por chunk)

InputStore (abstração de origem dos arquivos)
  ├─ LocalFileStore  (disco, modo dev)
  └─ BlobStore       (Azure Blob Storage / Azurite, ranged reads)
```
