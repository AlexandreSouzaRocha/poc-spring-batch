#!/usr/bin/env bash
set -euo pipefail

LINES_PER_FILE=${1:?"uso: capacity-test.sh <linesPerFile> [recordLength] [timeoutSeconds] [coresPerContainer]"}
RECORD_LENGTH=${2:-260}
TIMEOUT_SECONDS=${3:-600}
CORES_PER_CONTAINER=${4:-3}
TOTAL_RECORDS=$((LINES_PER_FILE * 3))

echo "=== Teste de capacidade por container: 3 containers, ${LINES_PER_FILE} linhas/arquivo (${TOTAL_RECORDS} total), ${CORES_PER_CONTAINER} cores/container ==="

echo "limpando kafka e azurite..."
docker compose stop kafka azurite >/dev/null
docker compose rm -f kafka azurite >/dev/null
docker volume rm -f poc-spring-batch_azurite-data >/dev/null 2>&1 || true
docker compose -f docker-compose.yml -f docker-compose.perf.yml up -d kafka azurite >/dev/null
echo "aguardando kafka/azurite ficarem saudáveis..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' poc-kafka 2>/dev/null)" = "healthy" ] \
   && [ "$(docker inspect -f '{{.State.Health.Status}}' poc-azurite 2>/dev/null)" = "healthy" ]; do
  sleep 2
done

echo "parando app-4..app-10 (só app-1, app-2, app-3 ficam ativos, com mais CPU cada)..."
docker compose stop app-4 app-5 app-6 app-7 app-8 app-9 app-10 >/dev/null

echo "reiniciando app-1/2/3 (recriam o container blob e reconectam ao kafka)..."
docker compose restart app-1 app-2 app-3 >/dev/null

echo "aguardando app-1/2/3 ficarem prontos (health check)..."
for port in 8081 8082 8083; do
  until curl -fsS "http://localhost:${port}/actuator/health" 2>/dev/null | grep -q '"status":"UP"'; do
    sleep 2
  done
done

echo "alocando ${CORES_PER_CONTAINER} cores dedicados por container (app-1/2/3)..."
c0=$((CORES_PER_CONTAINER - 1))
c1=$((CORES_PER_CONTAINER))
c2=$((CORES_PER_CONTAINER * 2 - 1))
c3=$((CORES_PER_CONTAINER * 2))
c4=$((CORES_PER_CONTAINER * 3 - 1))
docker update --cpus "${CORES_PER_CONTAINER}" --cpuset-cpus "0-${c0}" poc-app-1 >/dev/null
docker update --cpus "${CORES_PER_CONTAINER}" --cpuset-cpus "${c1}-${c2}" poc-app-2 >/dev/null
docker update --cpus "${CORES_PER_CONTAINER}" --cpuset-cpus "${c3}-${c4}" poc-app-3 >/dev/null

echo "aguardando estabilização do grupo consumidor kafka após o restart..."
sleep 10

echo "geração distribuída: app-1/2/3 geram e publicam 1 dígito cada (0, 1, 2), com CPU liberada durante a geração..."
for port in 8081 8082 8083; do
  container=$(docker ps --filter "publish=${port}" --format '{{.Names}}')
  docker update --cpus "$((CORES_PER_CONTAINER + 1))" "${container}" >/dev/null
done

t0_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
digit=0
pids=()
for port in 8081 8082 8083; do
  curl -fsS -X POST "http://localhost:${port}/data/generate?linesPerDigit=${LINES_PER_FILE}&recordLength=${RECORD_LENGTH}&digit=${digit}" \
    -o "/tmp/cap_${digit}.json" 2>"/tmp/cap_${digit}.err" &
  pids+=($!)
  digit=$((digit + 1))
done
gen_failed=0
for pid in "${pids[@]}"; do
  wait "$pid" || gen_failed=1
done
if [ "$gen_failed" -ne 0 ]; then
  echo "ERRO na geração; saída de cada instância:"
  for f in /tmp/cap_*.err; do
    [ -s "$f" ] && echo "--- ${f} ---" && cat "$f"
  done
  exit 1
fi
echo "geração concluída"

docker update --cpus "${CORES_PER_CONTAINER}" --cpuset-cpus "0-${c0}" poc-app-1 >/dev/null
docker update --cpus "${CORES_PER_CONTAINER}" --cpuset-cpus "${c1}-${c2}" poc-app-2 >/dev/null
docker update --cpus "${CORES_PER_CONTAINER}" --cpuset-cpus "${c3}-${c4}" poc-app-3 >/dev/null

WARMUP_SECONDS=${WARMUP_SECONDS:-15}
echo "aguardando ${WARMUP_SECONDS}s para o azurite estabilizar..."
sleep "${WARMUP_SECONDS}"

generate_done_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
gen_elapsed_ms=$((generate_done_ms - t0_ms))

echo "aguardando processamento (timeout ${TIMEOUT_SECONDS}s)..."
deadline=$((SECONDS + TIMEOUT_SECONDS))
completed=0
while [ $SECONDS -lt $deadline ]; do
  completed=$(
    set +o pipefail
    docker compose logs app-1 app-2 app-3 2>/dev/null \
      | grep -oE '[0-9]{13}_part_[0-9]\.dat processado com sucesso' \
      | grep -oE '^[0-9]{13}_part_[0-9]\.dat' \
      | awk -F'_part_' -v t0="$t0_ms" '($1+0) >= (t0+0) {print $2}' \
      | sort -u \
      | wc -l | tr -d ' '
  )
  errors=$(docker compose logs app-1 app-2 app-3 2>/dev/null | grep -c "CRÍTICO" || true)
  if [ "$completed" -ge 3 ]; then
    break
  fi
  sleep 2
done
t1_ms=$(python3 -c 'import time; print(int(time.time()*1000))')

processing_elapsed_ms=$((t1_ms - generate_done_ms))
total_elapsed_ms=$((t1_ms - t0_ms))
total_elapsed_sec=$(echo "scale=2; ${total_elapsed_ms}/1000" | bc)
tps=$(echo "scale=0; ${TOTAL_RECORDS}/(${processing_elapsed_ms}/1000)" | bc)
tps_per_container=$(echo "scale=0; ${tps}/3" | bc)

echo "arquivos concluídos: ${completed}/3 (críticos/órfãos: ${errors:-0})"
echo "tempo geração: $((gen_elapsed_ms))ms"
echo "tempo processamento (evento->conclusão): $((processing_elapsed_ms))ms"
echo "tempo total: ${total_elapsed_sec}s"
echo "throughput agregado (3 containers): ${tps} registros/s"
echo "throughput por container: ${tps_per_container} registros/s"

if [ "$completed" -lt 3 ]; then
  echo "AVISO: timeout atingido antes de concluir os 3 arquivos"
fi

echo "reativando app-4..app-10..."
docker compose start app-4 app-5 app-6 app-7 app-8 app-9 app-10 >/dev/null

echo "=== RESULT lines_per_file=${LINES_PER_FILE} total_records=${TOTAL_RECORDS} record_length=${RECORD_LENGTH} cores_per_container=${CORES_PER_CONTAINER} gen_ms=${gen_elapsed_ms} processing_ms=${processing_elapsed_ms} total_ms=${total_elapsed_ms} tps=${tps} tps_per_container=${tps_per_container} completed=${completed} ==="
