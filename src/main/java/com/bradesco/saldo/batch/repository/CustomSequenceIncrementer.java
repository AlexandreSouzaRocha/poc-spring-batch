package com.bradesco.saldo.batch.repository;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;

public class CustomSequenceIncrementer implements DataFieldMaxValueIncrementer {

    private static final String SEQUENCES_COLLECTION_NAME = "batch_sequences";

    private final RetryTemplate retryTemplate = new RetryTemplate(
            RetryPolicy.builder().includes(DataIntegrityViolationException.class).build());

    private final MongoOperations mongoOperations;
    private final String sequenceName;

    public CustomSequenceIncrementer(MongoOperations mongoOperations, String sequenceName) {
        this.mongoOperations = mongoOperations;
        this.sequenceName = sequenceName;
    }

    @Override
    public long nextLongValue() throws DataAccessException {
        try {
            return retryTemplate
                    .execute(() -> mongoOperations.execute(SEQUENCES_COLLECTION_NAME, collection -> collection
                            .findOneAndUpdate(new Document("_id", sequenceName), new Document("$inc", new Document("count", 1)),
                                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER))
                            .getLong("count")));
        } catch (RetryException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DataAccessException ex) {
                throw ex;
            }
            throw new RuntimeException("Falha ao obter o próximo valor da sequência " + sequenceName, e);
        }
    }

    @Override
    public int nextIntValue() throws DataAccessException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String nextStringValue() throws DataAccessException {
        throw new UnsupportedOperationException();
    }
}
