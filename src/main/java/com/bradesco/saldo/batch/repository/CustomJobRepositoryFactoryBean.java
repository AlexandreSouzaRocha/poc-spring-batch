package com.bradesco.saldo.batch.repository;

import org.springframework.batch.core.repository.support.AbstractJobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.util.Assert;

public class CustomJobRepositoryFactoryBean extends AbstractJobRepositoryFactoryBean implements InitializingBean {

    private MongoOperations mongoOperations;
    private DataFieldMaxValueIncrementer jobInstanceIncrementer;
    private DataFieldMaxValueIncrementer jobExecutionIncrementer;
    private DataFieldMaxValueIncrementer stepExecutionIncrementer;

    public void setMongoOperations(MongoOperations mongoOperations) {
        this.mongoOperations = mongoOperations;
    }

    @Override
    protected CustomJobInstanceDao createJobInstanceDao() {
        CustomJobInstanceDao dao = new CustomJobInstanceDao(this.mongoOperations);
        dao.setJobKeyGenerator(this.jobKeyGenerator);
        dao.setJobInstanceIncrementer(this.jobInstanceIncrementer);
        return dao;
    }

    @Override
    protected CustomJobExecutionDao createJobExecutionDao() {
        CustomJobExecutionDao dao = new CustomJobExecutionDao(this.mongoOperations);
        dao.setJobExecutionIncrementer(this.jobExecutionIncrementer);
        return dao;
    }

    @Override
    protected CustomStepExecutionDao createStepExecutionDao() {
        CustomStepExecutionDao dao = new CustomStepExecutionDao(this.mongoOperations);
        dao.setStepExecutionIncrementer(this.stepExecutionIncrementer);
        return dao;
    }

    @Override
    protected CustomExecutionContextDao createExecutionContextDao() {
        return new CustomExecutionContextDao(this.mongoOperations);
    }

    @Override
    protected Object getTarget() throws Exception {
        CustomJobInstanceDao jobInstanceDao = createJobInstanceDao();
        CustomJobExecutionDao jobExecutionDao = createJobExecutionDao();
        jobExecutionDao.setJobInstanceDao(jobInstanceDao);
        CustomStepExecutionDao stepExecutionDao = createStepExecutionDao();
        stepExecutionDao.setJobExecutionDao(jobExecutionDao);
        CustomExecutionContextDao executionContextDao = createExecutionContextDao();
        return new SimpleJobRepository(jobInstanceDao, jobExecutionDao, stepExecutionDao, executionContextDao);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        Assert.notNull(this.mongoOperations, "MongoOperations must not be null.");
        if (this.jobInstanceIncrementer == null) {
            this.jobInstanceIncrementer = new CustomSequenceIncrementer(this.mongoOperations, "batch_job_instance_seq");
        }
        if (this.jobExecutionIncrementer == null) {
            this.jobExecutionIncrementer = new CustomSequenceIncrementer(this.mongoOperations, "batch_job_execution_seq");
        }
        if (this.stepExecutionIncrementer == null) {
            this.stepExecutionIncrementer = new CustomSequenceIncrementer(this.mongoOperations, "batch_step_execution_seq");
        }
    }
}
