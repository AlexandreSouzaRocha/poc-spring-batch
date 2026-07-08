# docs

## Collection do Postman

`poc-spring-batch.postman_collection.json` — endpoints da POC.

**Importar:** Postman → _Import_ → selecione o arquivo. A collection traz as variáveis
`app1Url` (`http://localhost:8081`), `app2Url` (`http://localhost:8082`) e `run` (`poc`),
editáveis em _Variables_.

**Requests:**
| Request | Método | Endpoint | Descrição |
|---------|--------|----------|-----------|
| Gerar arquivo de teste (via app-1) | POST | `/data/generate` | Gera os 10 arquivos (260 bytes/linha) no storage configurado (blob ou disco). |
| Disparar batch — app-1 (0-4) | POST | `/batch/trigger?run={{run}}` | Dispara o job (dígitos 0-4) em background, responde `202`. |
| Disparar batch — app-2 (5-9) | POST | `/batch/trigger?run={{run}}` | Dispara o job (dígitos 5-9) em background, responde `202`. |
| Health — app-1 / app-2 | GET | `/actuator/health` | Health de cada container. |

**Fluxo:** `make up` (infra + app-1 + app-2) → _Gerar arquivo de teste_ → _Disparar batch_
(app-1 e app-2). Acompanhe as mensagens no kafka-ui em http://localhost:8080 (tópico
`saldo-contas`).
