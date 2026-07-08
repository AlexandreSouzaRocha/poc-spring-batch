package com.bradesco.saldo.batch.config;

import java.util.concurrent.Executors;

import com.bradesco.saldo.batch.model.AccountRecord;
import com.bradesco.saldo.batch.partition.InputFilesRangePartitioner;
import com.bradesco.saldo.batch.processor.LineProcessor;
import com.bradesco.saldo.batch.reader.ByteRangeLineReader;
import com.bradesco.saldo.batch.writer.KafkaLineWriter;
import org.apache.kafka.common.errors.RetriableException;

import org.springframework.batch.core.configuration.support.MongoDefaultBatchConfiguration;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

@Configuration
public class BatchConfig extends MongoDefaultBatchConfiguration {

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory mongoDatabaseFactory) {
        return new MongoTransactionManager(mongoDatabaseFactory);
    }

    @Bean
    static BeanPostProcessor mongoMapKeyDotReplacement() {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                if (bean instanceof MappingMongoConverter converter) {
                    converter.setMapKeyDotReplacement("#");
                }
                return bean;
            }
        };
    }

    @Bean
    public TaskExecutor partitionTaskExecutor() {
        return new TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Bean
    public TaskExecutor jobLaunchExecutor() {
        return new TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Bean
    public InputFilesRangePartitioner partitioner(
            @Value("${app.input-dir}") String inputDir,
            @Value("${app.partitions-per-file}") int partitionsPerFile) {
        return new InputFilesRangePartitioner(inputDir, partitionsPerFile);
    }

    @Bean
    public Step workerStep(JobRepository jobRepository,
                           MongoTransactionManager transactionManager,
                           ByteRangeLineReader reader,
                           LineProcessor processor,
                           KafkaLineWriter writer,
                           @Value("${app.chunk-size}") int chunkSize) {
        return new StepBuilder("workerStep", jobRepository)
                .<String, AccountRecord>chunk(chunkSize)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .transactionManager(transactionManager)
                .faultTolerant()
                .retryLimit(3)
                .retry(RetriableException.class)
                .build();
    }

    @Bean
    public Step masterStep(JobRepository jobRepository,
                           Step workerStep,
                           InputFilesRangePartitioner partitioner,
                           TaskExecutor partitionTaskExecutor,
                           @Value("${app.partitions-per-file}") int partitionsPerFile) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep);
        handler.setTaskExecutor(partitionTaskExecutor);
        handler.setGridSize(partitionsPerFile);
        try {
            handler.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao configurar o partition handler", e);
        }
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("workerStep", partitioner)
                .partitionHandler(handler)
                .build();
    }

    @Bean
    public Job saldoBatchJob(JobRepository jobRepository, Step masterStep) {
        return new JobBuilder("saldoBatchJob", jobRepository)
                .start(masterStep)
                .build();
    }
}
