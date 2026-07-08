package com.bradesco.saldo.batch.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class BatchLauncherService {

    private static final Logger log = LoggerFactory.getLogger(BatchLauncherService.class);

    private final JobOperator jobOperator;
    private final Job saldoBatchJob;
    private final TaskExecutor jobLaunchExecutor;
    private final String inputDir;
    private final String shard;

    public BatchLauncherService(JobOperator jobOperator,
                                Job saldoBatchJob,
                                @Qualifier("jobLaunchExecutor") TaskExecutor jobLaunchExecutor,
                                @Value("${app.input-dir}") String inputDir,
                                @Value("${app.digit-from}") int digitFrom,
                                @Value("${app.digit-to}") int digitTo) {
        this.jobOperator = jobOperator;
        this.saldoBatchJob = saldoBatchJob;
        this.jobLaunchExecutor = jobLaunchExecutor;
        this.inputDir = inputDir;
        this.shard = digitFrom + "-" + digitTo;
    }

    public void launchAsync(String run) {
        JobParameters parameters = new JobParametersBuilder()
                .addString("inputDir", inputDir)
                .addString("shard", shard)
                .addString("run", run)
                .toJobParameters();

        jobLaunchExecutor.execute(() -> {
            long start = System.currentTimeMillis();
            try {
                JobExecution execution = startWithRetry(parameters);
                double secs = (System.currentTimeMillis() - start) / 1000.0;
                var workers = execution.getStepExecutions().stream()
                        .filter(se -> se.getStepName().startsWith("workerStep"))
                        .toList();
                long read = workers.stream().mapToLong(se -> se.getReadCount()).sum();
                long written = workers.stream().mapToLong(se -> se.getWriteCount()).sum();
                long skipped = workers.stream().mapToLong(se -> se.getSkipCount()).sum();
                long tps = secs > 0 ? Math.round(written / secs) : 0;
                log.info("METRICS job=saldoBatchJob shard={} run={} status={} durationSec={} particoes={} "
                                + "lidos={} publicados={} skips={} tps={}",
                        shard, run, execution.getStatus(), String.format("%.2f", secs), workers.size(),
                        read, written, skipped, tps);
            } catch (Exception e) {
                log.error("METRICS job=saldoBatchJob shard={} run={} status=ERROR erro=\"{}\"",
                        shard, run, e.getMessage(), e);
            }
        });
    }

    // Transações de metadados no Mongo podem abortar com TransientTransactionError quando
    // múltiplos containers iniciam jobs ao mesmo tempo; o próprio Mongo instrui a retentar.
    private JobExecution startWithRetry(JobParameters parameters) throws Exception {
        int maxAttempts = 5;
        for (int attempt = 1; ; attempt++) {
            try {
                return jobOperator.start(saldoBatchJob, parameters);
            } catch (Exception e) {
                if (attempt >= maxAttempts || !isTransientMongoError(e)) {
                    throw e;
                }
                log.warn("Start abortado por erro transiente do Mongo (tentativa {}/{}); retentando...",
                        attempt, maxAttempts);
                Thread.sleep(250L * attempt);
            }
        }
    }

    private boolean isTransientMongoError(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("TransientTransactionError") || msg.contains("NoSuchTransaction")
                    || msg.contains("WriteConflict"))) {
                return true;
            }
        }
        return false;
    }
}
