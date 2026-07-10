package br.com.saldo.batch.repository;

import java.util.Collections;
import java.util.List;

import br.com.saldo.batch.repository.document.JobInstanceDocument;

import org.springframework.batch.core.job.DefaultJobKeyGenerator;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.JobKeyGenerator;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.persistence.converter.JobInstanceConverter;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.util.Assert;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class CustomJobInstanceDao implements JobInstanceDao {

    private static final String COLLECTION_NAME = "batch_job_instance";
    private static final String SEQUENCE_NAME = "batch_job_instance_seq";

    private final MongoOperations mongoOperations;
    private final JobInstanceConverter jobInstanceConverter = new JobInstanceConverter();
    private DataFieldMaxValueIncrementer jobInstanceIncrementer;
    private JobKeyGenerator jobKeyGenerator = new DefaultJobKeyGenerator();

    public CustomJobInstanceDao(MongoOperations mongoOperations) {
        Assert.notNull(mongoOperations, "mongoOperations must not be null.");
        this.mongoOperations = mongoOperations;
        this.jobInstanceIncrementer = new CustomSequenceIncrementer(mongoOperations, SEQUENCE_NAME);
    }

    public void setJobKeyGenerator(JobKeyGenerator jobKeyGenerator) {
        this.jobKeyGenerator = jobKeyGenerator;
    }

    public void setJobInstanceIncrementer(DataFieldMaxValueIncrementer jobInstanceIncrementer) {
        this.jobInstanceIncrementer = jobInstanceIncrementer;
    }

    @Override
    public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        Assert.state(getJobInstance(jobName, jobParameters) == null, "JobInstance must not already exist");

        org.springframework.batch.core.repository.persistence.JobInstance jobInstanceToSave =
                new org.springframework.batch.core.repository.persistence.JobInstance();
        jobInstanceToSave.setJobName(jobName);
        String key = this.jobKeyGenerator.generateKey(jobParameters);
        jobInstanceToSave.setJobKey(key);
        long instanceId = jobInstanceIncrementer.nextLongValue();
        jobInstanceToSave.setJobInstanceId(instanceId);
        this.mongoOperations.insert(DocumentMapper.toDocument(jobInstanceToSave), COLLECTION_NAME);

        JobInstance jobInstance = new JobInstance(instanceId, jobName);
        jobInstance.incrementVersion();
        return jobInstance;
    }

    @Override
    public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        String key = this.jobKeyGenerator.generateKey(jobParameters);
        Query query = query(where("jobName").is(jobName).and("jobKey").is(key));
        JobInstanceDocument document = this.mongoOperations.findOne(query, JobInstanceDocument.class, COLLECTION_NAME);
        return document != null ? this.jobInstanceConverter.toJobInstance(DocumentMapper.fromDocument(document)) : null;
    }

    @Override
    public JobInstance getJobInstance(long instanceId) {
        Query query = query(where("jobInstanceId").is(instanceId));
        JobInstanceDocument document = this.mongoOperations.findOne(query, JobInstanceDocument.class, COLLECTION_NAME);
        return document != null ? this.jobInstanceConverter.toJobInstance(DocumentMapper.fromDocument(document)) : null;
    }

    @Override
    public JobInstance getJobInstance(JobExecution jobExecution) {
        return getJobInstance(jobExecution.getJobInstanceId());
    }

    @Override
    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        if (count == 0) {
            return Collections.emptyList();
        }
        Query query = query(where("jobName").is(jobName)).with(Sort.by(Sort.Order.desc("jobInstanceId")))
                .skip(start)
                .limit(count);
        return this.mongoOperations
                .find(query, JobInstanceDocument.class, COLLECTION_NAME)
                .stream()
                .map(document -> this.jobInstanceConverter.toJobInstance(DocumentMapper.fromDocument(document)))
                .toList();
    }

    @Override
    public List<JobInstance> getJobInstances(String jobName) {
        Query query = query(where("jobName").is(jobName));
        return this.mongoOperations
                .find(query, JobInstanceDocument.class, COLLECTION_NAME)
                .stream()
                .map(document -> this.jobInstanceConverter.toJobInstance(DocumentMapper.fromDocument(document)))
                .toList();
    }

    @Override
    public List<Long> getJobInstanceIds(String jobName) {
        Query query = query(where("jobName").is(jobName));
        return this.mongoOperations
                .find(query, JobInstanceDocument.class, COLLECTION_NAME)
                .stream()
                .map(JobInstanceDocument::jobInstanceId)
                .toList();
    }

    public List<JobInstance> findJobInstancesByName(String jobName) {
        Query query = query(where("jobName").is(jobName));
        return this.mongoOperations
                .find(query, JobInstanceDocument.class, COLLECTION_NAME)
                .stream()
                .map(document -> this.jobInstanceConverter.toJobInstance(DocumentMapper.fromDocument(document)))
                .toList();
    }

    @Override
    public JobInstance getLastJobInstance(String jobName) {
        Query query = query(where("jobName").is(jobName));
        Sort.Order sortOrder = Sort.Order.desc("jobInstanceId");
        JobInstanceDocument document = this.mongoOperations.findOne(
                query.with(Sort.by(sortOrder)), JobInstanceDocument.class, COLLECTION_NAME);
        return document != null ? this.jobInstanceConverter.toJobInstance(DocumentMapper.fromDocument(document)) : null;
    }

    @Override
    public List<String> getJobNames() {
        Query query = new Query().with(Sort.by(Sort.Order.asc("jobName")));
        return this.mongoOperations.findDistinct(query, "jobName", COLLECTION_NAME, JobInstanceDocument.class, String.class);
    }

    @SuppressWarnings("removal")
    @Deprecated(forRemoval = true)
    @Override
    public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        return getJobInstances(jobName, start, count);
    }

    @Override
    public long getJobInstanceCount(String jobName) throws NoSuchJobException {
        if (!getJobNames().contains(jobName)) {
            throw new NoSuchJobException("No job instances were found for job name " + jobName);
        }
        Query query = query(where("jobName").is(jobName));
        return this.mongoOperations.count(query, JobInstanceDocument.class, COLLECTION_NAME);
    }

    @Override
    public void deleteJobInstance(JobInstance jobInstance) {
        this.mongoOperations.remove(query(where("jobInstanceId").is(jobInstance.getId())), JobInstanceDocument.class,
                COLLECTION_NAME);
    }
}
