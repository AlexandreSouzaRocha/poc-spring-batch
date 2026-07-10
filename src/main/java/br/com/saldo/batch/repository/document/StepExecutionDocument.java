package br.com.saldo.batch.repository.document;

import java.time.LocalDateTime;

import org.springframework.batch.core.BatchStatus;
import org.springframework.data.mongodb.core.mapping.Field;

public record StepExecutionDocument(
        @Field("step_execution_id") long stepExecutionId,
        @Field("job_execution_id") long jobExecutionId,
        @Field("name") String name,
        @Field("status") BatchStatus status,
        @Field("read_count") long readCount,
        @Field("write_count") long writeCount,
        @Field("commit_count") long commitCount,
        @Field("rollback_count") long rollbackCount,
        @Field("read_skip_count") long readSkipCount,
        @Field("process_skip_count") long processSkipCount,
        @Field("write_skip_count") long writeSkipCount,
        @Field("filter_count") long filterCount,
        @Field("start_time") LocalDateTime startTime,
        @Field("create_time") LocalDateTime createTime,
        @Field("end_time") LocalDateTime endTime,
        @Field("last_updated") LocalDateTime lastUpdated,
        @Field("execution_context") ExecutionContextDocument executionContext,
        @Field("exit_status") ExitStatusDocument exitStatus,
        @Field("terminate_only") boolean terminateOnly) {
}
