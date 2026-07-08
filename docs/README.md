# docs

## Collection do Postman

`poc-spring-batch.postman_collection.json` — endpoints da POC.

**Importar:** Postman → _Import_ → selecione o arquivo. A collection traz a variável
`baseUrl` (`http://localhost:8081`) e `run` (`poc`), editáveis em _Variables_.

**Requests:**
| Request | Método | Endpoint | Descrição |
|---------|--------|----------|-----------|
| Gerar arquivo de teste | POST | `/data/generate` | Gera os 10 arquivos (260 bytes/linha). Síncrono. |
| Disparar batch (fire-and-forget) | POST | `/batch/trigger` | Dispara o job em background, responde `202`. |
| Disparar batch — retomar | POST | `/batch/trigger?run={{run}}` | Reutiliza `run` para retomar de onde parou. |
| Health | GET | `/actuator/health` | Health da aplicação. |

**Fluxo:** `make up` (infra) → `make run` (app) → _Gerar arquivo de teste_ → _Disparar batch_.
Acompanhe as mensagens no kafka-ui em http://localhost:8080 (tópico `saldo-contas`).
