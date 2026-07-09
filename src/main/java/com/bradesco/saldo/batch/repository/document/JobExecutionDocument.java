package com.bradesco.saldo.batch.repository.document;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import org.springframework.batch.core.BatchStatus;
import org.springframework.data.mongodb.core.mapping.Field;

public record JobExecutionDocument(
        @Field("job_execution_id") long jobExecutionId,
        @Field("job_instance_id") long jobInstanceId,
        @Field("job_parameters") Set<JobParameterDocument> jobParameters,
        @Field("step_executions") List<StepExecutionDocument> stepExecutions,
        @Field("status") BatchStatus status,
        @Field("start_time") LocalDateTime startTime,
        @Field("create_time") LocalDateTime createTime,
        @Field("end_time") LocalDateTime endTime,
        @Field("last_updated") LocalDateTime lastUpdated,
        @Field("exit_status") ExitStatusDocument exitStatus,
        @Field("execution_context") ExecutionContextDocument executionContext) {
}
