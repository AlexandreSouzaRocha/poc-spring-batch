#!/usr/bin/env bash
# Gera a massa de teste chamando o endpoint da aplicação (precisa estar no ar).
#   ./scripts/generate-data.sh [linhasPorDigito] [recordLength]
# Padrão: 100000 linhas/dígito (1MM total), 260 bytes/linha.
# Destino: local (./data) ou Azure Blob (Azurite), conforme app.storage do app alvo.
set -euo pipefail

LINES_PER_DIGIT="${1:-100000}"
RECORD_LENGTH="${2:-260}"
APP_URL="${APP_URL:-http://localhost:8081}"

URL="${APP_URL}/data/generate?linesPerDigit=${LINES_PER_DIGIT}&recordLength=${RECORD_LENGTH}"

echo "POST ${URL}"
curl -fsS -X POST "${URL}"
echo
