package com.bradesco.saldo.batch.repository.document;

import java.util.Map;

import org.springframework.data.mongodb.core.mapping.Field;

public record ExecutionContextDocument(
        @Field("map") Map<String, Object> map,
        @Field("dirty") boolean dirty) {
}
