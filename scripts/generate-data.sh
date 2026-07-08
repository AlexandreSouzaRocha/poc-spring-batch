#!/usr/bin/env bash
# Gera a massa de teste chamando o endpoint da aplicação (precisa estar no ar).
#   ./scripts/generate-data.sh [linhasPorDigito] [recordLength] [dir]
# Padrão: 100000 linhas/dígito (1MM total), 260 bytes/linha, dir = ./data (default do app)
set -euo pipefail

LINES_PER_DIGIT="${1:-100000}"
RECORD_LENGTH="${2:-260}"
DIR="${3:-}"
APP_URL="${APP_URL:-http://localhost:8081}"

URL="${APP_URL}/data/generate?linesPerDigit=${LINES_PER_DIGIT}&recordLength=${RECORD_LENGTH}"
if [[ -n "${DIR}" ]]; then
  URL="${URL}&dir=${DIR}"
fi

echo "POST ${URL}"
curl -fsS -X POST "${URL}"
echo
