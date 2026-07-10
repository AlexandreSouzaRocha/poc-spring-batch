package br.com.saldo.batch.support;

import java.time.Duration;
import java.util.Date;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Component
public class DistributedLock {

    private static final String COLLECTION = "processing_locks";

    private final MongoTemplate mongoTemplate;
    private final Duration ttl;

    public DistributedLock(MongoTemplate mongoTemplate,
                           @Value("${app.file-processor.lock-ttl-seconds}") long ttlSeconds) {
        this.mongoTemplate = mongoTemplate;
        this.ttl = Duration.ofSeconds(ttlSeconds);
    }

    @PostConstruct
    void ensureIndex() {
        mongoTemplate.indexOps(COLLECTION)
                .ensureIndex(new Index().on("acquiredAt", Sort.Direction.ASC).expire(ttl));
    }

    public boolean tryAcquire(String key, String owner) {
        try {
            mongoTemplate.insert(new LockDocument(key, owner, new Date()), COLLECTION);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    public void release(String key, String owner) {
        Query query = Query.query(Criteria.where("_id").is(key).and("owner").is(owner));
        mongoTemplate.remove(query, COLLECTION);
    }

    private record LockDocument(String id, String owner, Date acquiredAt) {
    }
}
