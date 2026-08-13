#!/usr/bin/env bash
# Inicia o replica set rs0 (idempotente) e cria/seed das coleções de metadados do
# Spring Batch (nomes customizados em snake_case via CustomJobRepositoryFactoryBean).
# As sequences precisam existir com count=0 porque o CustomSequenceIncrementer
# faz findOneAndUpdate SEM upsert.
# Cria também os índices TTL que fazem o expurgo automático do histórico do
# JobRepository. batch_sequences fica de fora de propósito: é controle de sequência.
set -euo pipefail

DB="saldo_batch"
HOST="mongo:27017"
RETENTION_DAYS="${BATCH_RETENTION_DAYS:-7}"
TTL_SCRIPT="$(dirname "$0")/mongo-ttl-indexes.js"

echo "[mongo-init] aguardando mongod em ${HOST}..."
until mongosh --host "${HOST}" --quiet --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do
  sleep 1
done

echo "[mongo-init] inicializando replica set rs0 (se necessário)..."
mongosh --host "${HOST}" --quiet --eval '
  try {
    rs.status();
    print("replica set já inicializado");
  } catch (e) {
    rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "mongo:27017" }] });
    print("replica set inicializado");
  }
'

echo "[mongo-init] aguardando eleição do primary..."
until mongosh --host "${HOST}" --quiet --eval "rs.isMaster().ismaster" | grep -q true; do
  sleep 1
done

echo "[mongo-init] criando coleções e sequences batch_* em ${DB}..."
mongosh "mongodb://${HOST}/${DB}?directConnection=true" --quiet --eval '
  ["batch_job_instance","batch_job_execution","batch_step_execution","batch_sequences"].forEach(function(c){
    if (!db.getCollectionNames().includes(c)) { db.createCollection(c); }
  });
  ["batch_job_instance_seq","batch_job_execution_seq","batch_step_execution_seq"].forEach(function(id){
    db.batch_sequences.updateOne({ _id: id }, { $setOnInsert: { count: NumberLong("0") } }, { upsert: true });
  });
  print("metadados do Spring Batch prontos");
'

echo "[mongo-init] expurgo automático: TTL de ${RETENTION_DAYS} dia(s) nas coleções batch_*..."
BATCH_RETENTION_DAYS="${RETENTION_DAYS}" \
  mongosh "mongodb://${HOST}/${DB}?directConnection=true" --quiet --file "${TTL_SCRIPT}"

echo "[mongo-init] concluído."
