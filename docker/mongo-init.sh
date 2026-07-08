#!/usr/bin/env bash
# Inicia o replica set rs0 (idempotente) e cria/seed das coleções de metadados do
# Spring Batch. As sequences precisam existir com count=0 porque o
# MongoSequenceIncrementer faz findOneAndUpdate SEM upsert.
set -euo pipefail

DB="saldo_batch"
HOST="mongo:27017"

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

echo "[mongo-init] criando coleções e sequences BATCH_* em ${DB}..."
mongosh "mongodb://${HOST}/${DB}?directConnection=true" --quiet --eval '
  ["BATCH_JOB_INSTANCE","BATCH_JOB_EXECUTION","BATCH_STEP_EXECUTION","BATCH_SEQUENCES"].forEach(function(c){
    if (!db.getCollectionNames().includes(c)) { db.createCollection(c); }
  });
  ["BATCH_JOB_INSTANCE_SEQ","BATCH_JOB_EXECUTION_SEQ","BATCH_STEP_EXECUTION_SEQ"].forEach(function(id){
    db.BATCH_SEQUENCES.updateOne({ _id: id }, { $setOnInsert: { count: NumberLong("0") } }, { upsert: true });
  });
  print("metadados do Spring Batch prontos");
'

echo "[mongo-init] concluído."
