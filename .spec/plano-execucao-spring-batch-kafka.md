# POC Spring Batch — Processamento de ~50MM registros → Kafka

## Context

Precisamos validar, em POC, o processamento de arquivos de saldo enviados pelo mainframe
(~50MM de registros no total). Os arquivos vêm **particionados pelo dígito verificador da conta
(0–9)** — ou seja, até 10 arquivos, cada um com contas cujo DV é o mesmo dígito.

Cada linha segue o layout fixo:

```
BISD YYYY-MM-DDT23:59:59.9999990000 AAAA CCCCCCC
└──┘ └───────────────────────────┘ └──┘ └─────┘
prefixo   data/timestamp (literal)   ag.  conta
```
`AAAA` = agência (4 díg.), `CCCCCCC` = conta (7 díg.).

Objetivo: máximo TPS via **Spring Batch particionado + virtual threads (Java 21)**, processando em
chunks de 5000 linhas (flexível), com **resiliência/restart de onde parou**, publicando cada linha no
Kafka como `{"timestamp": <epoch ms da publicação>, "text": "<linha>"}` com **partition key `AAAA-CCCCCCC`**.
Toda a infra deve subir localmente via Docker Compose.

O repositório hoje é um esqueleto: `Application.java` vazio, `application.yaml` vazio, `pom.xml` já com
`spring-boot-starter-batch` e `spring-boot-starter-kafka` (Spring Boot 4.1.0, `java.version=21`).

### Decisões confirmadas com o usuário
- **Java 21** (asdf) — necessário para virtual threads; fixar no projeto via `.tool-versions`.
- **Partition key Kafka** = `AAAA-CCCCCCC` (agência-conta).
- **Volume de teste** = gerador **configurável**, padrão **1MM** (100k por dígito), escalável até 50MM.

---

## Arquitetura

```
docker-compose (infra local)
 ├─ kafka (KRaft, 1 broker)      topic: saldo-contas (N partições)
 ├─ mongo (replica set rs0)      JobRepository do Spring Batch (restart/resiliência)
 └─ kafka-ui                     observabilidade (localhost:8080)

App Spring Batch (roda no host, Java 21)
 Job: saldoBatchJob
  └─ masterStep (partitioned)
       Partitioner: InputFilesRangePartitioner
         → para cada arquivo (0..9) × N sub-ranges por byte-offset
         → gera 10*N partições, cada uma com {file, startByte, endByte}
       PartitionHandler: TaskExecutorPartitionHandler
         → TaskExecutor = virtual-thread-per-task (Executors.newVirtualThreadPerTaskExecutor)
       workerStep (chunk=5000, faultTolerant, restartable):
         reader:    ByteRangeLineReader  (ItemStream, salva offset no ExecutionContext)
         processor: LineProcessor         (linha → AccountRecord{key, text})
         writer:    KafkaLineWriter        (monta JSON com timestamp=now, envia c/ key, flush no fim do chunk)
```

**Por que particionar por byte-range e não só por arquivo:** com 10 arquivos teríamos no máx. 10
partições. Dividindo cada arquivo em N sub-ranges (ex.: N=8 → 80 partições) as virtual threads processam
o arquivo em paralelo, maximizando TPS. Cada partição continua sendo um chunk-step **sequencial e
restartável** (posição salva no `ExecutionContext`), evitando o problema de não-restartabilidade do
multi-threaded step do Spring Batch.

**Resiliência / restart:** `JobRepository` no **MongoDB** persiste `StepExecution` + `ExecutionContext` por
partição (coleções `BATCH_*`). Ao re-executar o mesmo `JobInstance` (mesmos params), partições `COMPLETED`
são puladas e a partição que falhou retoma do último chunk commitado (offset de byte salvo).
`faultTolerant()` com retry para erros transitórios do producer. Semântica **at-least-once** (idempotência
do producer ligada; possível duplicar itens do chunk em curso num crash — aceitável para POC).

> **Por que MongoDB e não Postgres:** desde o Spring Batch 5.2 existe suporte oficial a JobRepository em
> MongoDB (`MongoJobRepositoryFactoryBean`), presente no Batch 6 (Boot 4.1). Persistência de metadados e
> resiliência equivalentes ao relacional, sem depender de banco JDBC.
>
> ⚠️ **Requisito crítico:** o JobRepository do Mongo usa **transações multi-documento**, que no MongoDB só
> funcionam em **replica set** (não em standalone). O Mongo do compose sobe com `--replSet rs0` + `rs.initiate()`
> (nó único já basta). Em standalone o batch quebra na primeira gravação de metadado. Precisa também de um
> `MongoTransactionManager` como transaction manager do step.

### Estrutura das collections do JobRepository (MongoDB)

O `schema-mongodb.jsonl` do spring-batch-core cria **4 collections**. Mapeamento para este POC:

| Collection | Papel | Qtde de docs no POC |
|-----------|-------|---------------------|
| `BATCH_JOB_INSTANCE` | identidade lógica do job (nome + hash dos params) | 1 por conjunto de params |
| `BATCH_JOB_EXECUTION` | cada tentativa de rodar o job (+1 a cada restart) | 1 por execução |
| `BATCH_STEP_EXECUTION` | **cada partição** — guarda o offset de restart | **80** (10 arquivos × 8 ranges) |
| `BATCH_SEQUENCES` | contadores de ID (substitui auto-increment do SQL) | 3 docs fixos |

Diferença chave vs. JDBC: `jobParameters`, `executionContext` e `stepExecutions` ficam **embutidos** no
documento, em vez de espalhados em várias tabelas.

`BATCH_JOB_INSTANCE`:
```
{ "jobInstanceId": 1, "jobName": "saldoBatchJob", "jobKey": "a1b2c3…", "version": 0 }
```

`BATCH_JOB_EXECUTION`:
```
{
  "jobExecutionId": 1, "jobInstanceId": 1, "version": 3, "status": "STARTED",
  "startTime": ISODate("…"), "createTime": ISODate("…"),
  "endTime": null, "lastUpdated": ISODate("…"),
  "exitStatus": { "exitCode": "UNKNOWN", "exitDescription": "" },
  "jobParameters": [ { "name": "inputDir", "value": "./data", "identifying": true },
                     { "name": "run.id",  "value": "1",      "identifying": true } ],
  "executionContext": { "map": { } }
}
```

`BATCH_STEP_EXECUTION` — ⭐ o coração da resiliência (uma partição = um documento; o offset de byte que o
`ByteRangeLineReader` salva a cada commit fica em `executionContext.map`):
```
{
  "stepExecutionId": 42, "jobExecutionId": 1,
  "name": "workerStep:partition7", "status": "COMPLETED",
  "readCount": 125000, "writeCount": 125000, "commitCount": 25,
  "rollbackCount": 0, "readSkipCount": 0, "processSkipCount": 0,
  "writeSkipCount": 0, "filterCount": 0,
  "startTime": ISODate("…"), "endTime": ISODate("…"), "lastUpdated": ISODate("…"),
  "exitStatus": { "exitCode": "COMPLETED" },
  "executionContext": {
    "map": {
      "ByteRangeLineReader.bytesRead": 4718592,
      "ByteRangeLineReader.linesRead": 125000
    }
  }
}
```

`BATCH_SEQUENCES`:
```
{ "_id": "BATCH_JOB_INSTANCE_SEQ",  "count": 1  }
{ "_id": "BATCH_JOB_EXECUTION_SEQ", "count": 1  }
{ "_id": "BATCH_STEP_EXECUTION_SEQ","count": 80 }
```

**Como o restart usa isso:** no re-run com os mesmos params, o Batch reencontra o mesmo
`BATCH_JOB_INSTANCE` (via `jobKey`); partições com `status: COMPLETED` em `BATCH_STEP_EXECUTION` são puladas;
a partição que falhou lê `executionContext.map.bytesRead`, faz `seek()` e retoma do último chunk commitado.

**Virtual threads / TPS:** paralelismo = nº de partições rodando concorrentes em vthreads; o gargalo real
é o producer Kafka (batching) e o parsing. Producer configurado com `linger.ms`, `batch.size`,
`compression=lz4`, `acks=all`, `enable.idempotence=true`.

---

## Arquivos a criar / alterar

### Infra & build
- **`.tool-versions`** → `java temurin-21.x` (fixa Java 21 no projeto).
- **`pom.xml`** → adicionar `spring-boot-starter-data-mongodb` e `spring-boot-starter-actuator`
  (métricas/health). Manter batch + kafka já presentes. *(Fallback: se `spring-boot-starter-kafka` não
  resolver no Boot 4.1, trocar por `org.springframework.kafka:spring-kafka`.)*
- **`docker-compose.yml`** → serviços `kafka` (apache/kafka KRaft), `mongo:7` **em replica set**
  (`command: ["--replSet","rs0"]` + healthcheck/init que roda `rs.initiate()` uma vez), `kafka-ui`.
  Init do topic `saldo-contas` (via `KAFKA_AUTO_CREATE` ou pequeno container `kafka-topics`).

### Geração de dados
- **`scripts/GenerateData.java`** — single-file source (roda `java scripts/GenerateData.java <linhasPorDigito> <outDir>`).
  Para cada dígito d∈0..9 gera `part_d.dat` com `linhasPorDigito` linhas; conta termina em `d`
  (DV = último dígito); usa `BufferedWriter` para performance. Padrão 100_000 (→1MM); `500_000`×10 = 50MM.
- **`scripts/generate-data.sh`** — wrapper: valida Java 21, chama o gerador, imprime totais.

### Código (`com.bradesco.saldo.batch`)
- **`config/BatchConfig.java`** — estende `DefaultBatchConfiguration` (Batch 6): sobrescreve o
  `JobRepository` via `MongoJobRepositoryFactoryBean` (usa `MongoTemplate` + `MongoTransactionManager`) e
  expõe o `MongoTransactionManager` como o transaction manager dos steps. Beans: `Job saldoBatchJob`,
  `Step masterStep`, `Step workerStep` (chunk 5000, faultTolerant, retry), `PartitionHandler` com
  `TaskExecutor` virtual-thread, `JobParametersIncrementer` (run.id). Lê knobs de `application.yaml`.
- **`partition/InputFilesRangePartitioner.java`** — `Partitioner` que varre o `input-dir`, e para cada
  arquivo cria `partitions-per-file` ranges por byte-offset; popula `{fileName,startByte,endByte}`.
- **`reader/ByteRangeLineReader.java`** — `ItemStreamReader<String>`: abre o arquivo, faz seek em
  `startByte`, descarta linha parcial inicial (se start>0), lê linhas até passar `endByte`; salva
  `bytesRead`/`linesRead` no `ExecutionContext` para restart. `@StepScope`, lê params via `#{stepExecutionContext[...]}`.
- **`processor/LineProcessor.java`** — extrai `AAAA` e `CCCCCCC` (posições fixas), monta
  `AccountRecord(key="AAAA-CCCCCCC", text=linhaCompleta)`.
- **`writer/KafkaLineWriter.java`** — `ItemWriter<AccountRecord>`: para cada item monta
  `KafkaMessage(timestamp=System.currentTimeMillis(), text)`, envia com `KafkaTemplate` usando a key;
  `flush()`/join dos futures no fim do chunk antes do commit (durabilidade p/ restart).
- **`model/AccountRecord.java`**, **`model/KafkaMessage.java`** — records.
- **`runner/JobLauncherRunner.java`** — `CommandLineRunner` que lança `saldoBatchJob` com params
  `{inputDir, run.id}` (desabilitar auto-run default e controlar aqui, ou usar auto-run do Boot).

### Config
- **`src/main/resources/application.yaml`** — `app.input-dir`, `app.chunk-size:5000`,
  `app.partitions-per-file:8`, `app.kafka-topic:saldo-contas`; `spring.threads.virtual.enabled:true`;
  `spring.data.mongodb.uri` (`mongodb://localhost:27017/saldo_batch?replicaSet=rs0`);
  `spring.kafka.producer` (key=StringSerializer, value=JsonSerializer, acks=all, idempotence, linger/batch/compression);
  `management.endpoints` (health, metrics).
  *(As coleções de metadados `BATCH_*` + a coleção de sequences do Mongo são criadas na 1ª execução;
  se necessário, incluir o script de init de coleções/sequences do Spring Batch no compose.)*

---

## Verificação (end-to-end)

1. **Subir infra:** `docker compose up -d` → conferir kafka, mongo (replica set `rs0` iniciado) e
   kafka-ui (localhost:8080) saudáveis.
2. **Gerar dados:** `./scripts/generate-data.sh 100000 ./data` → 10 arquivos, 1MM linhas.
3. **Rodar app (Java 21):** `./mvnw spring-boot:run` → logs mostram partições lançadas (10×8), chunks
   commitados, throughput.
4. **Validar Kafka:** kafka-ui → topic `saldo-contas` com 1MM mensagens; inspecionar uma mensagem:
   value `{"timestamp":<epoch ms>,"text":"BISD…AAAACCCCCCC"}`, key `AAAA-CCCCCCC`.
5. **Testar resiliência:** matar o app no meio (`Ctrl+C`), reexecutar com os mesmos params → confirmar
   retomada: no `mongosh`, `db.BATCH_STEP_EXECUTION.find({}, {stepName:1, status:1})` mostra partições
   `COMPLETED` puladas; total de mensagens converge para 1MM (admitindo duplicatas at-least-once do chunk em curso).
6. **Escalar/tunar:** `./scripts/generate-data.sh 500000 ./data` (50MM); ajustar `chunk-size`,
   `partitions-per-file` e batching do producer; medir TPS via logs/actuator metrics.

## Riscos / notas
- Java 21 **obrigatório** — o app não sobe em Java 17 (virtual threads + baseline do pom). `.tool-versions` resolve.
- 50MM completo gera arquivos grandes (~vários GB) e leva minutos para gerar/processar; iterar em 1MM.
- **MongoDB precisa estar em replica set** para o JobRepository funcionar (transações multi-documento). Um
  Mongo standalone parece subir, mas o batch falha na 1ª gravação de metadado. O compose já sobe com `--replSet rs0`.
- Semântica at-least-once: duplicatas possíveis num crash; se exactly-once for requisito futuro, avaliar
  Kafka transactions integradas à transação do chunk (fora do escopo desta POC).
- Confirmar resolução de `spring-boot-starter-kafka` no Boot 4.1; fallback `spring-kafka` documentado acima.
