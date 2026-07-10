#!/usr/bin/env bash
# Retoma a matriz de comparação a partir do ponto onde parou (10c p8/c5000 1MM já concluído).
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

# Limpeza automática: acima de 70% de uso na VM do Docker Desktop, prune containers
# parados + volumes órfãos. Evita repetir a corrupção de disco (I/O error) que já
# aconteceu 2x nesta sessão quando o disco encheu durante um teste de 200MM.
# Usa poc-mongo (sempre de pé nesse ponto do script) pra ler o filesystem da VM,
# em vez de `docker run --rm alpine` (que exige pull e já falhou por I/O error antes).
check_disk() {
  local usage
  usage=$(docker exec poc-mongo df -P / 2>/dev/null | awk 'NR==2 {gsub("%","",$5); print $5}')
  if [ -n "${usage:-}" ] && [ "${usage}" -ge 70 ] 2>/dev/null; then
    echo "  [disco em ${usage}%, limpando containers parados + volumes órfãos antes de continuar...]"
    docker container prune -f >/dev/null 2>&1
    docker volume prune -f >/dev/null 2>&1
    local after
    after=$(docker exec poc-mongo df -P / 2>/dev/null | awk 'NR==2 {gsub("%","",$5); print $5}')
    echo "  [disco agora em ${after:-desconhecido}%]"
  fi
}

run_10c() {
  local partitions=$1 chunk=$2 vol=$3 timeout=$4
  local tag="p${partitions}_c${chunk}_${vol}"
  if [ -f "/tmp/matrix/10c_${tag}.log" ] && grep -q '^=== RESULT' "/tmp/matrix/10c_${tag}.log" 2>/dev/null; then
    echo "=== [10 containers] p=${partitions} c=${chunk} vol=${vol} JÁ FEITO, pulando ==="
    return
  fi
  echo "=== [10 containers] partitions=${partitions} chunk=${chunk} volume=${vol} ==="
  check_disk
  ./scripts/benchmark.sh "$vol" 260 "$timeout" > "/tmp/matrix/10c_${tag}.log" 2>&1
  echo "  -> $(grep '^=== RESULT' "/tmp/matrix/10c_${tag}.log" || echo 'SEM RESULTADO (falhou)')"
}

run_3c() {
  local partitions=$1 chunk=$2 linesPerFile=$3 timeout=$4 total=$5
  local tag="p${partitions}_c${chunk}_${total}"
  if [ -f "/tmp/matrix/3c_${tag}.log" ] && grep -q '^=== RESULT' "/tmp/matrix/3c_${tag}.log" 2>/dev/null; then
    echo "=== [3 containers] p=${partitions} c=${chunk} total=${total} JÁ FEITO, pulando ==="
    return
  fi
  echo "=== [3 containers] partitions=${partitions} chunk=${chunk} total=${total} (linesPerFile=${linesPerFile}) ==="
  check_disk
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
