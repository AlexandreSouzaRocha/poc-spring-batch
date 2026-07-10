package br.com.saldo.batch.service;

import br.com.saldo.batch.support.MongoRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class BatchLauncherService {

    private static final Logger log = LoggerFactory.getLogger(BatchLauncherService.class);

    private final JobOperator jobOperator;
    private final Job saldoFileJob;
    private final TaskExecutor jobLaunchExecutor;

    public BatchLauncherService(JobOperator jobOperator,
                                Job saldoFileJob,
                                @Qualifier("jobLaunchExecutor") TaskExecutor jobLaunchExecutor) {
        this.jobOperator = jobOperator;
        this.saldoFileJob = saldoFileJob;
        this.jobLaunchExecutor = jobLaunchExecutor;
    }

    public void launchFileAsync(String fileName) {
        JobParameters parameters = new JobParametersBuilder()
                .addString("inputFile", fileName)
                .toJobParameters();

        jobLaunchExecutor.execute(() -> runAndLog(saldoFileJob, parameters, "saldoFileJob file=" + fileName));
    }

    private void runAndLog(Job job, JobParameters parameters, String label) {
        long start = System.currentTimeMillis();
        try {
            JobExecution execution = MongoRetry.withRetry(() -> jobOperator.start(job, parameters));
            double secs = (System.currentTimeMillis() - start) / 1000.0;
            var workers = execution.getStepExecutions().stream()
                    .filter(se -> se.getStepName().startsWith("workerStep"))
                    .toList();
            long read = workers.stream().mapToLong(se -> se.getReadCount()).sum();
            long written = workers.stream().mapToLong(se -> se.getWriteCount()).sum();
            long skipped = workers.stream().mapToLong(se -> se.getSkipCount()).sum();
            long tps = secs > 0 ? Math.round(written / secs) : 0;
            log.info("METRICS job={} status={} durationSec={} particoes={} lidos={} publicados={} skips={} tps={}",
                    label, execution.getStatus(), String.format("%.2f", secs), workers.size(),
                    read, written, skipped, tps);
        } catch (Exception e) {
            log.error("METRICS job={} status=ERROR erro=\"{}\"", label, e.getMessage(), e);
        }
    }
}
