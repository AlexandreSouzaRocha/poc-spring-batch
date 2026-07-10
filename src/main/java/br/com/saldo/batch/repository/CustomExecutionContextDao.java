package br.com.saldo.batch.repository;

import java.util.Collection;
import java.util.Collections;

import br.com.saldo.batch.repository.document.JobExecutionDocument;
import br.com.saldo.batch.repository.document.StepExecutionDocument;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class CustomExecutionContextDao implements ExecutionContextDao {

    private static final String STEP_EXECUTIONS_COLLECTION_NAME = "batch_step_execution";
    private static final String JOB_EXECUTIONS_COLLECTION_NAME = "batch_job_execution";

    private final MongoOperations mongoOperations;

    public CustomExecutionContextDao(MongoOperations mongoOperations) {
        this.mongoOperations = mongoOperations;
    }

    @Override
    public ExecutionContext getExecutionContext(JobExecution jobExecution) {
        Query query = query(where("jobExecutionId").is(jobExecution.getId()));
        JobExecutionDocument execution = this.mongoOperations.findOne(
                query, JobExecutionDocument.class, JOB_EXECUTIONS_COLLECTION_NAME);
        if (execution == null) {
            return new ExecutionContext();
        }
        return new ExecutionContext(execution.executionContext().map());
    }

    @Override
    public ExecutionContext getExecutionContext(StepExecution stepExecution) {
        Query query = query(where("stepExecutionId").is(stepExecution.getId()));
        StepExecutionDocument execution = this.mongoOperations.findOne(
                query, StepExecutionDocument.class, STEP_EXECUTIONS_COLLECTION_NAME);
        if (execution == null) {
            return new ExecutionContext();
        }
        return new ExecutionContext(execution.executionContext().map());
    }

    @Override
    public void saveExecutionContext(JobExecution jobExecution) {
        ExecutionContext executionContext = jobExecution.getExecutionContext();
        Query query = query(where("jobExecutionId").is(jobExecution.getId()));

        Update update = Update.update("executionContext",
                DocumentMapper.toDocument(new org.springframework.batch.core.repository.persistence.ExecutionContext(
                        executionContext.toMap(), executionContext.isDirty())));
        this.mongoOperations.updateFirst(query, update, JobExecutionDocument.class, JOB_EXECUTIONS_COLLECTION_NAME);
    }

    @Override
    public void saveExecutionContext(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        Query query = query(where("stepExecutionId").is(stepExecution.getId()));

        Update update = Update.update("executionContext",
                DocumentMapper.toDocument(new org.springframework.batch.core.repository.persistence.ExecutionContext(
                        executionContext.toMap(), executionContext.isDirty())));
        this.mongoOperations.updateFirst(query, update, StepExecutionDocument.class, STEP_EXECUTIONS_COLLECTION_NAME);
    }

    @Override
    public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {
        for (StepExecution stepExecution : stepExecutions) {
            saveExecutionContext(stepExecution);
        }
    }

    @Override
    public void updateExecutionContext(JobExecution jobExecution) {
        saveExecutionContext(jobExecution);
    }

    @Override
    public void updateExecutionContext(StepExecution stepExecution) {
        saveExecutionContext(stepExecution);
    }

    @Override
    public void deleteExecutionContext(JobExecution jobExecution) {
        Query query = new Query(where("jobExecutionId").is(jobExecution.getId()));
        var executionContext = DocumentMapper.toDocument(
                new org.springframework.batch.core.repository.persistence.ExecutionContext(Collections.emptyMap(), false));
        Update executionContextRemovalUpdate = new Update().set("executionContext", executionContext);
        this.mongoOperations.updateFirst(query, executionContextRemovalUpdate, JobExecutionDocument.class,
                JOB_EXECUTIONS_COLLECTION_NAME);
    }

    @Override
    public void deleteExecutionContext(StepExecution stepExecution) {
        Query query = new Query(where("stepExecutionId").is(stepExecution.getId()));
        var executionContext = DocumentMapper.toDocument(
                new org.springframework.batch.core.repository.persistence.ExecutionContext(Collections.emptyMap(), false));
        Update executionContextRemovalUpdate = new Update().set("executionContext", executionContext);
        this.mongoOperations.updateFirst(query, executionContextRemovalUpdate, StepExecutionDocument.class,
                STEP_EXECUTIONS_COLLECTION_NAME);
    }
}
