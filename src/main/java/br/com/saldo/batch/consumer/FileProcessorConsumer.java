package br.com.saldo.batch.consumer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import br.com.saldo.batch.storage.InputStore;
import br.com.saldo.batch.support.DistributedLock;
import br.com.saldo.batch.support.MongoRetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class FileProcessorConsumer {

    private static final Logger log = LoggerFactory.getLogger(FileProcessorConsumer.class);
    private static final String JOB_NAME = "saldoFileJob";
    private static final int MAX_IDENTITY_VARIANTS = 5;

    private final JobOperator jobOperator;
    private final Job saldoFileJob;
    private final JobRepository jobRepository;
    private final InputStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DistributedLock lock;
    private final String ownerId;
    private final String dlqTopic;
    private final int maxAttempts;
    private final Duration retryBackoff;

    public FileProcessorConsumer(JobOperator jobOperator,
                                 Job saldoFileJob,
                                 JobRepository jobRepository,
                                 InputStore store,
                                 KafkaTemplate<String, String> kafkaTemplate,
                                 DistributedLock lock,
                                 @Value("${app.file-processor.dlq-topic}") String dlqTopic,
                                 @Value("${app.file-processor.max-attempts}") int maxAttempts,
                                 @Value("${app.file-processor.retry-backoff-seconds}") long retryBackoffSeconds) {
        this.jobOperator = jobOperator;
        this.saldoFileJob = saldoFileJob;
        this.jobRepository = jobRepository;
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
        this.lock = lock;
        this.ownerId = System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString());
        this.dlqTopic = dlqTopic;
        this.maxAttempts = maxAttempts;
        this.retryBackoff = Duration.ofSeconds(retryBackoffSeconds);
    }

    @KafkaListener(topics = "${app.file-processor.topic}", groupId = "${app.file-processor.group-id}")
    public void onFileReady(String fileName, Acknowledgment ack) {
        if (!lock.tryAcquire(fileName, ownerId)) {
            log.info("Arquivo {} está com lock ativo em outro container; retentando", fileName);
            ack.nack(retryBackoff);
            return;
        }
        try {
            processFile(fileName, ack);
        } finally {
            lock.release(fileName, ownerId);
        }
    }

    private void processFile(String fileName, Acknowledgment ack) {
        WorkingIdentity identity = resolveWorkingIdentity(fileName);
        if (identity == null) {
            log.error("CRÍTICO: {} variantes de identidade órfãs seguidas para {}; retentando "
                    + "(arquivo NÃO vai para o DLQ por problema de metadado)", MAX_IDENTITY_VARIANTS, fileName);
            ack.nack(retryBackoff);
            return;
        }

        long failedAttempts = identity.executions().stream()
                .filter(e -> e.getStatus() == BatchStatus.FAILED)
                .count();
        if (shouldSendToDlq(failedAttempts, maxAttempts)) {
            sendToDlq(fileName, failedAttempts);
            ack.acknowledge();
            return;
        }

        try {
            JobExecution execution = MongoRetry.withRetry(() -> jobOperator.start(saldoFileJob, identity.parameters()));
            if (execution.getStatus() == BatchStatus.COMPLETED) {
                log.info("Arquivo {} processado com sucesso (jobExecutionId={})",
                        fileName, execution.getId());
                ack.acknowledge();
            } else {
                log.warn("Job do arquivo {} terminou com status {}; será retentado", fileName, execution.getStatus());
                ack.nack(retryBackoff);
            }
        } catch (JobInstanceAlreadyCompleteException e) {
            log.info("Arquivo {} já tinha sido processado com sucesso antes (redelivery); confirmando sem reprocessar",
                    fileName);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Falha ao processar arquivo {}: {}", fileName, e.getMessage(), e);
            ack.nack(retryBackoff);
        }
    }

    private record WorkingIdentity(JobParameters parameters, List<JobExecution> executions) {
    }

    private WorkingIdentity resolveWorkingIdentity(String fileName) {
        for (int variant = 0; variant < MAX_IDENTITY_VARIANTS; variant++) {
            JobParametersBuilder builder = new JobParametersBuilder().addString("inputFile", fileName);
            if (variant > 0) {
                builder.addString("variant", Integer.toString(variant));
            }
            JobParameters parameters = builder.toJobParameters();

            JobInstance instance = jobRepository.getJobInstance(JOB_NAME, parameters);
            if (instance == null) {
                return new WorkingIdentity(parameters, List.of());
            }

            List<JobExecution> executions = jobRepository.getJobExecutions(instance);
            if (!executions.isEmpty()) {
                recoverAbandonedExecution(instance, executions);
                return new WorkingIdentity(parameters, executions);
            }

            log.warn("Identidade variant={} de {} está órfã (corrida na criação); tentando variant={}",
                    variant, fileName, variant + 1);
        }
        return null;
    }

    static boolean shouldSendToDlq(long failedAttempts, int maxAttempts) {
        return failedAttempts >= maxAttempts;
    }

    private void recoverAbandonedExecution(JobInstance instance, List<JobExecution> executions) {
        JobExecution last = executions.stream().max(Comparator.comparing(JobExecution::getId)).orElse(null);
        if (last == null) {
            return;
        }

        boolean hadDangling = false;
        for (StepExecution stepExecution : last.getStepExecutions()) {
            if (stepExecution.getStatus().isRunning()) {
                hadDangling = true;
                stepExecution.setStatus(BatchStatus.FAILED);
                stepExecution.setEndTime(LocalDateTime.now());
                stepExecution.setExitStatus(ExitStatus.FAILED);
                jobRepository.update(stepExecution);
            }
        }

        if (last.getStatus().isRunning()) {
            hadDangling = true;
            last.setStatus(BatchStatus.FAILED);
            last.setEndTime(LocalDateTime.now());
            last.setExitStatus(ExitStatus.FAILED);
        }

        if (hadDangling) {
            jobRepository.update(last);
            log.warn("Execução {} do arquivo {} tinha step(s)/execução presos em STARTING/STARTED/STOPPING "
                    + "(processo anterior morreu no meio ou erro transiente na gravação da falha); "
                    + "marcado como FAILED para retomar (snapshot embutido no job_execution atualizado)",
                    last.getId(), instance.getJobName());
        }
    }

    private void sendToDlq(String fileName, long failedAttempts) {
        log.error("Arquivo {} excedeu {} tentativas de processamento ({} falhas reais); movendo para "
                + "errors/ e enviando ao DLQ", fileName, maxAttempts, failedAttempts);
        try {
            store.moveToErrorFolder(fileName);
        } catch (Exception e) {
            log.error("Falha ao mover {} para a pasta de erros: {}", fileName, e.getMessage(), e);
        }
        kafkaTemplate.send(dlqTopic, fileName, fileName);
    }
}
