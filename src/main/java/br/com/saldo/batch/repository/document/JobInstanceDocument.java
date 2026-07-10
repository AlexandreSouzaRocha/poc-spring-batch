package br.com.saldo.batch.repository.document;

import org.springframework.data.mongodb.core.mapping.Field;

public record JobInstanceDocument(
        @Field("job_instance_id") long jobInstanceId,
        @Field("job_name") String jobName,
        @Field("job_key") String jobKey) {
}
