#!/usr/bin/env bash
# Roda a matriz completa de comparação: partitions-per-file (8, 10) x chunk-size (5000, 10000)
# x volumes, para 10 containers (topologia real) e 3 containers (teste de capacidade).
# Cada combinação grava um log em /tmp/matrix/ com nome autoexplicativo.
set -uo pipefail

cd "$(dirname "$0")/.."
mkdir -p /tmp/matrix

COMPOSE_FILE=docker-compose.yml

set_config() {
  local partitions=$1
  local chunk=$2
  sed -i '' "s/APP_PARTITIONS_PER_FILE: [0-9]*/APP_PARTITIONS_PER_FILE: ${partitions}/" "$COMPOSE_FILE"
  sed -i '' "s/APP_CHUNK_SIZE: [0-9]*/APP_CHUNK_SIZE: ${chunk}/" "$COMPOSE_FILE"
  docker compose -f docker-compose.yml -f docker-compose.perf.yml up -d >/dev/null 2>&1
  sleep 10
}

run_10c() {
  local partitions=$1 chunk=$2 vol=$3 timeout=$4
  local tag="p${partitions}_c${chunk}_${vol}"
  echo "=== [10 containers] partitions=${partitions} chunk=${chunk} volume=${vol} ==="
  ./scripts/benchmark.sh "$vol" 260 "$timeout" > "/tmp/matrix/10c_${tag}.log" 2>&1
  echo "  -> $(grep '^=== RESULT' "/tmp/matrix/10c_${tag}.log" || echo 'SEM RESULTADO (falhou)')"
}

run_3c() {
  local partitions=$1 chunk=$2 linesPerFile=$3 timeout=$4 total=$5
  local tag="p${partitions}_c${chunk}_${total}"
  echo "=== [3 containers] partitions=${partitions} chunk=${chunk} total=${total} (linesPerFile=${linesPerFile}) ==="
  ./scripts/capacity-test.sh "$linesPerFile" 260 "$timeout" 3 > "/tmp/matrix/3c_${tag}.log" 2>&1
  echo "  -> $(grep '^=== RESULT' "/tmp/matrix/3c_${tag}.log" || echo 'SEM RESULTADO (falhou)')"
}

for partitions in 8 10; do
  for chunk in 5000 10000; do
    echo ""
    echo "##### CONFIG: partitions-per-file=${partitions} chunk-size=${chunk} #####"
    set_config "$partitions" "$chunk"

    run_10c "$partitions" "$chunk" 1000000 300
    run_10c "$partitions" "$chunk" 5000000 300
    run_10c "$partitions" "$chunk" 10000000 400
    run_10c "$partitions" "$chunk" 20000000 500
    run_10c "$partitions" "$chunk" 100000000 1200

    run_3c "$partitions" "$chunk" 6000000 300 18000000
    run_3c "$partitions" "$chunk" 20000000 600 60000000
    run_3c "$partitions" "$chunk" 66666667 1800 200000000
  done
done

echo ""
echo "##### MATRIZ COMPLETA #####"
grep -H '^=== RESULT' /tmp/matrix/*.log
