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
RETENTION_SECONDS=$(( RETENTION_DAYS * 86400 ))

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

echo "[mongo-init] backfill de create_time em batch_job_instance..."
mongosh "mongodb://${HOST}/${DB}?directConnection=true" --quiet --eval '
  var res = db.batch_job_instance.updateMany(
    { create_time: { $exists: false } },
    [{ $set: { create_time: "$$NOW" } }]
  );
  print("instances sem create_time atualizadas: " + res.modifiedCount);
'

echo "[mongo-init] aplicando TTL de ${RETENTION_DAYS} dia(s) nas coleções batch_*..."
mongosh "mongodb://${HOST}/${DB}?directConnection=true" --quiet --eval '
  var retentionSeconds = '"${RETENTION_SECONDS}"';
  // create_time nas três: é o único campo que o Spring Batch sempre preenche na
  // criação. last_updated fica nulo entre o insert e o primeiro update, e um
  // container morto nessa janela deixaria o documento sem expirar nunca.
  // Como create_time cresce de instance -> job_execution -> step_execution, as
  // três expiram nessa ordem e a instance sempre sai primeiro.
  [
    { collection: "batch_job_instance",   field: "create_time" },
    { collection: "batch_job_execution",  field: "create_time" },
    { collection: "batch_step_execution", field: "create_time" }
  ].forEach(function(spec){
    var name = spec.field + "_ttl";
    var keys = {}; keys[spec.field] = 1;
    var coll = db.getCollection(spec.collection);

    coll.getIndexes().forEach(function(i){
      if (i.expireAfterSeconds !== undefined && i.name !== name) {
        coll.dropIndex(i.name);
        print("  removido " + spec.collection + "." + i.name + " (TTL obsoleto)");
      }
    });

    var existing = coll.getIndexes().filter(function(i){ return i.name === name; })[0];

    if (!existing) {
      coll.createIndex(keys, { name: name, expireAfterSeconds: retentionSeconds });
      print("  criado  " + spec.collection + "." + name + " (" + retentionSeconds + "s)");
    } else if (existing.expireAfterSeconds !== retentionSeconds) {
      // trocar expireAfterSeconds exige collMod: um createIndex com valor
      // diferente falharia com IndexOptionsConflict (erro 85).
      db.runCommand({ collMod: spec.collection, index: { name: name, expireAfterSeconds: retentionSeconds } });
      print("  ajustado " + spec.collection + "." + name + " -> " + retentionSeconds + "s");
    } else {
      print("  ok      " + spec.collection + "." + name + " (" + retentionSeconds + "s)");
    }
  });
'

echo "[mongo-init] concluído."
