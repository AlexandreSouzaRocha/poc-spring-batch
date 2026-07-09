package com.bradesco.saldo.batch.repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.bradesco.saldo.batch.repository.document.JobExecutionDocument;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.persistence.JobParameter;
import org.springframework.batch.core.repository.persistence.converter.JobExecutionConverter;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.util.CollectionUtils;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class CustomJobExecutionDao implements JobExecutionDao {

    private static final String JOB_EXECUTIONS_COLLECTION_NAME = "batch_job_execution";
    private static final String JOB_EXECUTIONS_SEQUENCE_NAME = "batch_job_execution_seq";

    private final MongoOperations mongoOperations;
    private final JobExecutionConverter jobExecutionConverter = new JobExecutionConverter();
    private DataFieldMaxValueIncrementer jobExecutionIncrementer;
    private CustomJobInstanceDao jobInstanceDao;

    public CustomJobExecutionDao(MongoOperations mongoOperations) {
        this.mongoOperations = mongoOperations;
        this.jobExecutionIncrementer = new CustomSequenceIncrementer(mongoOperations, JOB_EXECUTIONS_SEQUENCE_NAME);
    }

    public void setJobExecutionIncrementer(DataFieldMaxValueIncrementer jobExecutionIncrementer) {
        this.jobExecutionIncrementer = jobExecutionIncrementer;
    }

    public void setJobInstanceDao(CustomJobInstanceDao jobInstanceDao) {
        this.jobInstanceDao = jobInstanceDao;
    }

    public JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters) {
        long id = jobExecutionIncrementer.nextLongValue();
        JobExecution jobExecution = new JobExecution(id, jobInstance, jobParameters);

        org.springframework.batch.core.repository.persistence.JobExecution jobExecutionToSave = this.jobExecutionConverter
                .fromJobExecution(jobExecution);
        this.mongoOperations.insert(DocumentMapper.toDocument(jobExecutionToSave), JOB_EXECUTIONS_COLLECTION_NAME);

        return jobExecution;
    }

    @Override
    public void updateJobExecution(JobExecution jobExecution) {
        Query query = query(where("jobExecutionId").is(jobExecution.getId()));
        org.springframework.batch.core.repository.persistence.JobExecution jobExecutionToUpdate = this.jobExecutionConverter
                .fromJobExecution(jobExecution);
        this.mongoOperations.findAndReplace(query, DocumentMapper.toDocument(jobExecutionToUpdate), JOB_EXECUTIONS_COLLECTION_NAME);
    }

    @Override
    public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
        Query query = query(where("jobInstanceId").is(jobInstance.getId()))
                .with(Sort.by(Sort.Direction.DESC, "jobExecutionId"));
        List<JobExecutionDocument> jobExecutions = this.mongoOperations
                .find(query, JobExecutionDocument.class, JOB_EXECUTIONS_COLLECTION_NAME);
        return jobExecutions.stream()
                .map(document -> convert(DocumentMapper.fromDocument(document), jobInstance))
                .toList();
    }

    @Override
    public JobExecution getLastJobExecution(JobInstance jobInstance) {
        Query query = query(where("jobInstanceId").is(jobInstance.getId()));
        Sort.Order sortOrder = Sort.Order.desc("jobExecutionId");
        JobExecutionDocument document = this.mongoOperations.findOne(
                query.with(Sort.by(sortOrder)), JobExecutionDocument.class, JOB_EXECUTIONS_COLLECTION_NAME);
        return document != null ? convert(DocumentMapper.fromDocument(document), jobInstance) : null;
    }

    @Override
    public Set<JobExecution> findRunningJobExecutions(String jobName) {
        List<JobInstance> jobInstances = this.jobInstanceDao.findJobInstancesByName(jobName);
        if (jobInstances.isEmpty()) {
            return Collections.emptySet();
        }
        Map<Long, JobInstance> jobInstanceMap = jobInstances.stream()
                .collect(Collectors.toMap(JobInstance::getId, Function.identity()));
        Query query = query(
                where("jobInstanceId").in(jobInstanceMap.keySet()).and("status").in("STARTING", "STARTED", "STOPPING"));
        return this.mongoOperations
                .find(query, JobExecutionDocument.class, JOB_EXECUTIONS_COLLECTION_NAME)
                .stream()
                .map(document -> {
                    org.springframework.batch.core.repository.persistence.JobExecution jobExecution =
                            DocumentMapper.fromDocument(document);
                    return convert(jobExecution, jobInstanceMap.get(jobExecution.getJobInstanceId()));
                })
                .collect(Collectors.toSet());
    }

    @Override
    public JobExecution getJobExecution(long executionId) {
        Query jobExecutionQuery = query(where("jobExecutionId").is(executionId));
        JobExecutionDocument document = this.mongoOperations.findOne(
                jobExecutionQuery, JobExecutionDocument.class, JOB_EXECUTIONS_COLLECTION_NAME);
        if (document == null) {
            return null;
        }
        org.springframework.batch.core.repository.persistence.JobExecution jobExecution = DocumentMapper.fromDocument(document);
        JobInstance jobInstance = this.jobInstanceDao.getJobInstance(jobExecution.getJobInstanceId());
        return convert(jobExecution, jobInstance);
    }

    @Override
    public void synchronizeStatus(JobExecution jobExecution) {
        JobExecution currentJobExecution = getJobExecution(jobExecution.getId());
        if (currentJobExecution != null && currentJobExecution.getStatus().isGreaterThan(jobExecution.getStatus())) {
            jobExecution.upgradeStatus(currentJobExecution.getStatus());
        }
    }

    @Override
    public void deleteJobExecution(JobExecution jobExecution) {
        this.mongoOperations.remove(query(where("jobExecutionId").is(jobExecution.getId())), JobExecutionDocument.class,
                JOB_EXECUTIONS_COLLECTION_NAME);
    }

    @Override
    public void deleteJobExecutionParameters(JobExecution jobExecution) {
        Query query = new Query(where("jobExecutionId").is(jobExecution.getId()));
        Update jobParametersRemovalUpdate = new Update().set("jobParameters", Collections.emptyList());
        this.mongoOperations.updateFirst(query, jobParametersRemovalUpdate, JobExecutionDocument.class,
                JOB_EXECUTIONS_COLLECTION_NAME);
    }

    private JobExecution convert(org.springframework.batch.core.repository.persistence.JobExecution jobExecution,
                                 JobInstance jobInstance) {
        Set<JobParameter<?>> parameters = jobExecution.getJobParameters();
        if (!CollectionUtils.isEmpty(parameters)) {
            Set<JobParameter<?>> converted = new HashSet<>();
            for (JobParameter<?> parameter : parameters) {
                if (LocalDate.class.getName().equals(parameter.type()) && parameter.value() instanceof Date date) {
                    converted.add(new JobParameter<>(parameter.name(),
                            date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), parameter.type(),
                            parameter.identifying()));
                } else if (LocalTime.class.getName().equals(parameter.type()) && parameter.value() instanceof Date date) {
                    converted.add(new JobParameter<>(parameter.name(),
                            date.toInstant().atZone(ZoneId.systemDefault()).toLocalTime(), parameter.type(),
                            parameter.identifying()));
                } else if (LocalDateTime.class.getName().equals(parameter.type())
                        && parameter.value() instanceof Date date) {
                    converted.add(new JobParameter<>(parameter.name(),
                            date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime(), parameter.type(),
                            parameter.identifying()));
                } else {
                    converted.add(parameter);
                }
            }
            jobExecution.setJobParameters(converted);
        }
        return this.jobExecutionConverter.toJobExecution(jobExecution, jobInstance);
    }
}
