package com.bradesco.saldo.batch.repository.document;

import org.springframework.data.mongodb.core.mapping.Field;

public record ExitStatusDocument(
        @Field("exit_code") String exitCode,
        @Field("exit_description") String exitDescription) {
}
