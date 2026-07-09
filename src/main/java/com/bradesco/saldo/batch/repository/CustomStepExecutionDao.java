package com.bradesco.saldo.batch.repository;

import java.util.List;

import com.bradesco.saldo.batch.repository.document.JobExecutionDocument;
import com.bradesco.saldo.batch.repository.document.StepExecutionDocument;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.persistence.converter.JobExecutionConverter;
import org.springframework.batch.core.repository.persistence.converter.StepExecutionConverter;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class CustomStepExecutionDao implements StepExecutionDao {

    private static final String STEP_EXECUTIONS_COLLECTION_NAME = "batch_step_execution";
    private static final String STEP_EXECUTIONS_SEQUENCE_NAME = "batch_step_execution_seq";
    private static final String JOB_EXECUTIONS_COLLECTION_NAME = "batch_job_execution";

    private final StepExecutionConverter stepExecutionConverter = new StepExecutionConverter();
    private final JobExecutionConverter jobExecutionConverter = new JobExecutionConverter();
    private final MongoOperations mongoOperations;
    private DataFieldMaxValueIncrementer stepExecutionIncrementer;
    private CustomJobExecutionDao jobExecutionDao;

    public CustomStepExecutionDao(MongoOperations mongoOperations) {
        this.mongoOperations = mongoOperations;
        this.stepExecutionIncrementer = new CustomSequenceIncrementer(mongoOperations, STEP_EXECUTIONS_SEQUENCE_NAME);
    }

    public void setStepExecutionIncrementer(DataFieldMaxValueIncrementer stepExecutionIncrementer) {
        this.stepExecutionIncrementer = stepExecutionIncrementer;
    }

    public void setJobExecutionDao(CustomJobExecutionDao jobExecutionDao) {
        this.jobExecutionDao = jobExecutionDao;
    }

    @Override
    public StepExecution createStepExecution(String stepName, JobExecution jobExecution) {
        long id = stepExecutionIncrementer.nextLongValue();

        StepExecution stepExecution = new StepExecution(id, stepName, jobExecution);
        org.springframework.batch.core.repository.persistence.StepExecution stepExecutionToSave = this.stepExecutionConverter
                .fromStepExecution(stepExecution);
        this.mongoOperations.insert(DocumentMapper.toDocument(stepExecutionToSave), STEP_EXECUTIONS_COLLECTION_NAME);

        return stepExecution;
    }

    @Override
    public void updateStepExecution(StepExecution stepExecution) {
        Query query = query(where("stepExecutionId").is(stepExecution.getId()));
        org.springframework.batch.core.repository.persistence.StepExecution stepExecutionToUpdate = this.stepExecutionConverter
                .fromStepExecution(stepExecution);
        this.mongoOperations.findAndReplace(query, DocumentMapper.toDocument(stepExecutionToUpdate),
                STEP_EXECUTIONS_COLLECTION_NAME);
    }

    @Override
    public StepExecution getStepExecution(long stepExecutionId) {
        Query query = query(where("stepExecutionId").is(stepExecutionId));
        StepExecutionDocument document = this.mongoOperations
                .findOne(query, StepExecutionDocument.class, STEP_EXECUTIONS_COLLECTION_NAME);
        return document != null ? this.stepExecutionConverter.toStepExecution(DocumentMapper.fromDocument(document),
                jobExecutionDao.getJobExecution(document.jobExecutionId())) : null;
    }

    @Deprecated(since = "6.0", forRemoval = true)
    @Override
    public StepExecution getStepExecution(JobExecution jobExecution, long stepExecutionId) {
        Query query = query(where("stepExecutionId").is(stepExecutionId));
        StepExecutionDocument document = this.mongoOperations
                .findOne(query, StepExecutionDocument.class, STEP_EXECUTIONS_COLLECTION_NAME);
        return document != null
                ? this.stepExecutionConverter.toStepExecution(DocumentMapper.fromDocument(document), jobExecution)
                : null;
    }

    @Override
    public void synchronizeStatus(StepExecution stepExecution) {
        StepExecution currentStepExecution = getStepExecution(stepExecution.getId());
        if (currentStepExecution != null && currentStepExecution.getStatus().isGreaterThan(stepExecution.getStatus())) {
            stepExecution.upgradeStatus(currentStepExecution.getStatus());
        }
    }

    @Override
    public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
        Query jobExecutionsQuery = query(where("jobInstanceId").is(jobInstance.getId()));
        List<JobExecutionDocument> jobExecutions = this.mongoOperations
                .find(jobExecutionsQuery, JobExecutionDocument.class, JOB_EXECUTIONS_COLLECTION_NAME);
        if (jobExecutions.isEmpty()) {
            return null;
        }
        List<Long> jobExecutionIds = jobExecutions.stream()
                .map(JobExecutionDocument::jobExecutionId)
                .toList();
        Query stepExecutionQuery = query(where("jobExecutionId").in(jobExecutionIds).and("name").is(stepName))
                .with(Sort.by(Sort.Direction.DESC, "createTime", "stepExecutionId"))
                .limit(1);
        StepExecutionDocument lastStepExecutionDocument = this.mongoOperations
                .findOne(stepExecutionQuery, StepExecutionDocument.class, STEP_EXECUTIONS_COLLECTION_NAME);
        if (lastStepExecutionDocument == null) {
            return null;
        }
        JobExecutionDocument matchingJobExecutionDocument = jobExecutions.stream()
                .filter(execution -> execution.jobExecutionId() == lastStepExecutionDocument.jobExecutionId())
                .findFirst()
                .get();
        JobExecution jobExecution = this.jobExecutionConverter.toJobExecution(
                DocumentMapper.fromDocument(matchingJobExecutionDocument), jobInstance);
        return this.stepExecutionConverter.toStepExecution(DocumentMapper.fromDocument(lastStepExecutionDocument),
                jobExecution);
    }

    @Override
    public List<StepExecution> getStepExecutions(JobExecution jobExecution) {
        Query query = query(where("jobExecutionId").is(jobExecution.getId()));
        return this.mongoOperations
                .find(query, StepExecutionDocument.class, STEP_EXECUTIONS_COLLECTION_NAME)
                .stream()
                .map(document -> this.stepExecutionConverter.toStepExecution(DocumentMapper.fromDocument(document), jobExecution))
                .toList();
    }

    @Override
    public long countStepExecutions(JobInstance jobInstance, String stepName) {
        Query query = query(where("jobInstanceId").is(jobInstance.getId()));
        List<JobExecutionDocument> jobExecutions = this.mongoOperations
                .find(query, JobExecutionDocument.class, JOB_EXECUTIONS_COLLECTION_NAME);
        return this.mongoOperations.count(
                query(where("jobExecutionId")
                        .in(jobExecutions.stream()
                                .map(JobExecutionDocument::jobExecutionId)
                                .toList())
                        .and("name")
                        .is(stepName)),
                StepExecutionDocument.class,
                STEP_EXECUTIONS_COLLECTION_NAME);
    }

    @Override
    public void deleteStepExecution(StepExecution stepExecution) {
        this.mongoOperations.remove(query(where("stepExecutionId").is(stepExecution.getId())), StepExecutionDocument.class,
                STEP_EXECUTIONS_COLLECTION_NAME);
    }
}
