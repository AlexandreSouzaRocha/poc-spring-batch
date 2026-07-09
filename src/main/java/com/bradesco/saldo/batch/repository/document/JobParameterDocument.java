package com.bradesco.saldo.batch.repository.document;

import org.springframework.data.mongodb.core.mapping.Field;

public record JobParameterDocument(
        @Field("name") String name,
        @Field("value") Object value,
        @Field("type") String type,
        @Field("identifying") boolean identifying) {
}
