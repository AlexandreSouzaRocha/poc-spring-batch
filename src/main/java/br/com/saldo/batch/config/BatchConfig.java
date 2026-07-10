package br.com.saldo.batch.config;

import java.util.concurrent.Executors;

import br.com.saldo.batch.listener.StepMetricsListener;
import br.com.saldo.batch.model.AccountRecord;
import br.com.saldo.batch.partition.InputFilesRangePartitioner;
import br.com.saldo.batch.processor.LineProcessor;
import br.com.saldo.batch.reader.ByteRangeLineReader;
import br.com.saldo.batch.repository.CustomJobRepositoryFactoryBean;
import br.com.saldo.batch.storage.InputStore;
import br.com.saldo.batch.writer.KafkaLineWriter;
import jakarta.annotation.Nonnull;
import org.apache.kafka.common.errors.RetriableException;

import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.annotation.JobScope;
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
    @Override
    @Nonnull
    public JobRepository jobRepository() throws BatchConfigurationException {
        var factoryBean = new CustomJobRepositoryFactoryBean();
        try {
            factoryBean.setMongoOperations(getMongoOperations());
            factoryBean.setTransactionManager(getTransactionManager());
            factoryBean.setIsolationLevelForCreateEnum(getIsolationLevelForCreate());
            factoryBean.setValidateTransactionState(getValidateTransactionState());
            factoryBean.setJobKeyGenerator(getJobKeyGenerator());
            factoryBean.afterPropertiesSet();
            return factoryBean.getObject();
        } catch (Exception e) {
            throw new BatchConfigurationException("Unable to configure the custom job repository", e);
        }
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
    public Step workerStep(JobRepository jobRepository,
                           MongoTransactionManager transactionManager,
                           ByteRangeLineReader reader,
                           LineProcessor processor,
                           KafkaLineWriter writer,
                           StepMetricsListener stepMetricsListener,
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
                .listener(stepMetricsListener)
                .build();
    }

    @Bean
    @JobScope
    public InputFilesRangePartitioner fileEventPartitioner(
            InputStore inputStore,
            @Value("${app.partitions-per-file}") int partitionsPerFile,
            @Value("#{jobParameters['inputFile']}") String inputFile) {
        return InputFilesRangePartitioner.forSingleFile(inputStore, partitionsPerFile, inputFile);
    }

    @Bean
    public Step fileMasterStep(JobRepository jobRepository,
                               Step workerStep,
                               InputFilesRangePartitioner fileEventPartitioner,
                               TaskExecutor partitionTaskExecutor,
                               StepMetricsListener stepMetricsListener,
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
        return new StepBuilder("fileMasterStep", jobRepository)
                .partitioner("workerStep", fileEventPartitioner)
                .partitionHandler(handler)
                .listener(stepMetricsListener)
                .build();
    }

    @Bean
    public Job saldoFileJob(JobRepository jobRepository, Step fileMasterStep) {
        return new JobBuilder("saldoFileJob", jobRepository)
                .start(fileMasterStep)
                .build();
    }
}
