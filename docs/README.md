# docs

## Collection do Postman

`poc-spring-batch.postman_collection.json` — endpoints da POC.

**Importar:** Postman → _Import_ → selecione o arquivo. A collection traz as variáveis
`app1Url` (`http://localhost:8081`), `app2Url` (`http://localhost:8082`), `run` (`poc`)
e `file` (nome de exemplo), editáveis em _Variables_. A stack sobe **10 containers**
(`app-1`..`app-10`, portas `:8081`-`:8090`, 1 partição cada); a collection usa os dois
primeiros como exemplo representativo — qualquer endpoint funciona em qualquer porta.

**Requests:**
| Request | Método | Endpoint | Descrição |
|---------|--------|----------|-----------|
| Gerar arquivo de teste | POST | `/data/generate` | Gera os 10 arquivos (260 bytes/linha) e **dispara o processamento sozinho** via evento no tópico `saldo-file-processor`. |
| [Manual] Disparar batch — app-1 / app-2 | POST | `/batch/trigger?run={{run}}` | Reprocessamento ad-hoc de todos os dígitos 0-9 (via 1 container só já basta). Não é necessário no fluxo normal. |
| [Manual] Reprocessar um arquivo específico | POST | `/batch/trigger-file?file={{file}}` | Reprocessa um único arquivo (mesmo job do consumer) — útil após corrigir um arquivo que caiu no DLQ. |
| Health — app-1 / app-2 | GET | `/actuator/health` | Health de cada container. |

**Fluxo normal:** `make up` (infra + os 10 containers) → _Gerar arquivo de teste_ →
pronto, o processamento acontece sozinho. Acompanhe:
- kafka-ui (http://localhost:8080) → tópico `saldo-contas` (saída) e `saldo-file-processor` (entrada)
- `make consumer-group` → partições/lag do consumer group
- `make metrics` → progresso por container
