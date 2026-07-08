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

    public BatchLauncherService(JobOperator jobOperator,
                                Job saldoBatchJob,
                                @Qualifier("jobLaunchExecutor") TaskExecutor jobLaunchExecutor,
                                @Value("${app.input-dir}") String inputDir) {
        this.jobOperator = jobOperator;
        this.saldoBatchJob = saldoBatchJob;
        this.jobLaunchExecutor = jobLaunchExecutor;
        this.inputDir = inputDir;
    }

    public void launchAsync(String run) {
        JobParameters parameters = new JobParametersBuilder()
                .addString("inputDir", inputDir)
                .addString("run", run)
                .toJobParameters();

        jobLaunchExecutor.execute(() -> {
            long start = System.currentTimeMillis();
            try {
                JobExecution execution = jobOperator.start(saldoBatchJob, parameters);
                double secs = (System.currentTimeMillis() - start) / 1000.0;
                var workers = execution.getStepExecutions().stream()
                        .filter(se -> se.getStepName().startsWith("workerStep"))
                        .toList();
                long read = workers.stream().mapToLong(se -> se.getReadCount()).sum();
                long written = workers.stream().mapToLong(se -> se.getWriteCount()).sum();
                long skipped = workers.stream().mapToLong(se -> se.getSkipCount()).sum();
                long tps = secs > 0 ? Math.round(written / secs) : 0;
                log.info("METRICS job=saldoBatchJob run={} status={} durationSec={} particoes={} "
                                + "lidos={} publicados={} skips={} tps={}",
                        run, execution.getStatus(), String.format("%.2f", secs), workers.size(),
                        read, written, skipped, tps);
            } catch (Exception e) {
                log.error("METRICS job=saldoBatchJob run={} status=ERROR erro=\"{}\"", run, e.getMessage(), e);
            }
        });
    }
}
