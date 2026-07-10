package br.com.saldo.batch.listener;

import java.time.Duration;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

@Component
public class StepMetricsListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(StepMetricsListener.class);

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        LocalDateTime start = stepExecution.getStartTime();
        LocalDateTime end = stepExecution.getEndTime();
        double secs = (start != null && end != null)
                ? Duration.between(start, end).toMillis() / 1000.0
                : 0.0;
        long written = stepExecution.getWriteCount();
        long tps = secs > 0 ? Math.round(written / secs) : 0;

        log.info("STEP_METRICS step={} jobExecutionId={} status={} durationSec={} lidos={} publicados={} skips={} tps={}",
                stepExecution.getStepName(), stepExecution.getJobExecutionId(), stepExecution.getStatus(),
                String.format("%.2f", secs), stepExecution.getReadCount(), written,
                stepExecution.getSkipCount(), tps);

        return stepExecution.getExitStatus();
    }
}
