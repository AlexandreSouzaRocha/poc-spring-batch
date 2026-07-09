#!/usr/bin/env bash
set -euo pipefail

TOTAL_RECORDS=${1:?"uso: benchmark.sh <totalRecords> [recordLength] [timeoutSeconds]"}
RECORD_LENGTH=${2:-260}
TIMEOUT_SECONDS=${3:-3600}
APPS="app-1 app-2 app-3 app-4 app-5 app-6 app-7 app-8 app-9 app-10"
PORTS="8081 8082 8083 8084 8085 8086 8087 8088 8089 8090"
LINES_PER_DIGIT=$((TOTAL_RECORDS / 10))

echo "=== Benchmark: ${TOTAL_RECORDS} registros (${LINES_PER_DIGIT}/dígito, ${RECORD_LENGTH} bytes/linha) ==="

echo "limpando kafka (recriando container) e azurite (volume) para não acumular dados de execuções anteriores..."
docker compose stop kafka azurite >/dev/null
docker compose rm -f kafka azurite >/dev/null
docker volume rm -f poc-spring-batch_azurite-data >/dev/null 2>&1 || true
docker compose -f docker-compose.yml -f docker-compose.perf.yml up -d kafka azurite >/dev/null
echo "aguardando kafka/azurite ficarem saudáveis..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' poc-kafka 2>/dev/null)" = "healthy" ] \
   && [ "$(docker inspect -f '{{.State.Health.Status}}' poc-azurite 2>/dev/null)" = "healthy" ]; do
  sleep 2
done

echo "reiniciando containers da app (recriam o container blob e reconectam ao kafka)..."
docker compose restart ${APPS} >/dev/null
echo "aguardando apps ficarem prontos (health check)..."
for port in ${PORTS}; do
  until curl -fsS "http://localhost:${port}/actuator/health" 2>/dev/null | grep -q '"status":"UP"'; do
    sleep 2
  done
done
echo "aguardando estabilização do grupo consumidor kafka após o restart..."
sleep 10

echo "geração distribuída: cada instância gera e publica apenas o dígito correspondente ao seu container (mainframe simulado sem limite de CPU)"
for port in ${PORTS}; do
  container=$(docker ps --filter "publish=${port}" --format '{{.Names}}')
  docker update --cpus 2 "${container}" >/dev/null
done

t0_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
digit=0
pids=()
for port in ${PORTS}; do
  curl -fsS -X POST "http://localhost:${port}/data/generate?linesPerDigit=${LINES_PER_DIGIT}&recordLength=${RECORD_LENGTH}&digit=${digit}" \
    -o "/tmp/gen_${digit}.json" 2>"/tmp/gen_${digit}.err" &
  pids+=($!)
  digit=$((digit + 1))
done
gen_failed=0
for pid in "${pids[@]}"; do
  wait "$pid" || gen_failed=1
done
if [ "$gen_failed" -ne 0 ]; then
  echo "ERRO na geração distribuída; saída de cada instância:"
  for f in /tmp/gen_*.err; do
    [ -s "$f" ] && echo "--- ${f} ---" && cat "$f"
  done
  exit 1
fi
echo "geração concluída (10 instâncias em paralelo, 1 dígito cada)"

for port in ${PORTS}; do
  container=$(docker ps --filter "publish=${port}" --format '{{.Names}}')
  docker update --cpus 1 "${container}" >/dev/null
done

WARMUP_SECONDS=${WARMUP_SECONDS:-15}
echo "aguardando ${WARMUP_SECONDS}s para o azurite estabilizar após a carga de escrita (mainframe real gera com antecedência; aqui simulamos essa folga)..."
sleep "${WARMUP_SECONDS}"

generate_done_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
gen_elapsed_ms=$((generate_done_ms - t0_ms))

echo "aguardando processamento (timeout ${TIMEOUT_SECONDS}s)..."
deadline=$((SECONDS + TIMEOUT_SECONDS))
completed=0
while [ $SECONDS -lt $deadline ]; do
  completed=$(
    set +o pipefail
    docker compose logs ${APPS} 2>/dev/null \
      | grep -oE '[0-9]{13}_part_[0-9]\.dat processado com sucesso' \
      | grep -oE '^[0-9]{13}_part_[0-9]\.dat' \
      | awk -F'_part_' -v t0="$t0_ms" '($1+0) >= (t0+0) {print $2}' \
      | sort -u \
      | wc -l | tr -d ' '
  )
  errors=$(docker compose logs ${APPS} 2>/dev/null | grep -c "CRÍTICO" || true)
  if [ "$completed" -ge 10 ]; then
    break
  fi
  sleep 2
done
t1_ms=$(python3 -c 'import time; print(int(time.time()*1000))')

processing_elapsed_ms=$((t1_ms - generate_done_ms))
total_elapsed_ms=$((t1_ms - t0_ms))
total_elapsed_sec=$(echo "scale=2; ${total_elapsed_ms}/1000" | bc)
tps=$(echo "scale=0; ${TOTAL_RECORDS}/(${total_elapsed_ms}/1000)" | bc)

echo "arquivos concluídos: ${completed}/10 (críticos/órfãos: ${errors:-0})"
echo "tempo geração: $((gen_elapsed_ms))ms"
echo "tempo processamento (evento->conclusão): $((processing_elapsed_ms))ms"
echo "tempo total: ${total_elapsed_sec}s"
echo "throughput agregado: ${tps} registros/s"

if [ "$completed" -lt 10 ]; then
  echo "AVISO: timeout atingido antes de concluir os 10 arquivos"
fi

echo "=== RESULT total_records=${TOTAL_RECORDS} record_length=${RECORD_LENGTH} gen_ms=${gen_elapsed_ms} processing_ms=${processing_elapsed_ms} total_ms=${total_elapsed_ms} tps=${tps} completed=${completed} ==="
