package br.com.saldo.batch.repository;

import java.time.LocalDateTime;
import java.util.stream.Collectors;

import br.com.saldo.batch.repository.document.ExecutionContextDocument;
import br.com.saldo.batch.repository.document.ExitStatusDocument;
import br.com.saldo.batch.repository.document.JobExecutionDocument;
import br.com.saldo.batch.repository.document.JobInstanceDocument;
import br.com.saldo.batch.repository.document.JobParameterDocument;
import br.com.saldo.batch.repository.document.StepExecutionDocument;

import org.springframework.batch.core.repository.persistence.ExecutionContext;
import org.springframework.batch.core.repository.persistence.ExitStatus;
import org.springframework.batch.core.repository.persistence.JobExecution;
import org.springframework.batch.core.repository.persistence.JobInstance;
import org.springframework.batch.core.repository.persistence.JobParameter;
import org.springframework.batch.core.repository.persistence.StepExecution;

final class DocumentMapper {

    private DocumentMapper() {
    }

    static JobInstanceDocument toDocument(JobInstance jobInstance, LocalDateTime createTime) {
        return new JobInstanceDocument(jobInstance.getJobInstanceId(), jobInstance.getJobName(), jobInstance.getJobKey(),
                createTime);
    }

    static JobInstance fromDocument(JobInstanceDocument document) {
        if (document == null) {
            return null;
        }
        JobInstance jobInstance = new JobInstance();
        jobInstance.setJobInstanceId(document.jobInstanceId());
        jobInstance.setJobName(document.jobName());
        jobInstance.setJobKey(document.jobKey());
        return jobInstance;
    }

    static JobParameterDocument toDocument(JobParameter<?> parameter) {
        return new JobParameterDocument(parameter.name(), parameter.value(), parameter.type(), parameter.identifying());
    }

    static JobParameter<?> fromDocument(JobParameterDocument document) {
        return new JobParameter<>(document.name(), document.value(), document.type(), document.identifying());
    }

    static ExitStatusDocument toDocument(ExitStatus exitStatus) {
        if (exitStatus == null) {
            return null;
        }
        return new ExitStatusDocument(exitStatus.exitCode(), exitStatus.exitDescription());
    }

    static ExitStatus fromDocument(ExitStatusDocument document) {
        if (document == null) {
            return null;
        }
        return new ExitStatus(document.exitCode(), document.exitDescription());
    }

    static ExecutionContextDocument toDocument(ExecutionContext executionContext) {
        if (executionContext == null) {
            return null;
        }
        return new ExecutionContextDocument(executionContext.map(), executionContext.dirty());
    }

    static ExecutionContext fromDocument(ExecutionContextDocument document) {
        if (document == null) {
            return null;
        }
        return new ExecutionContext(document.map(), document.dirty());
    }

    static StepExecutionDocument toDocument(StepExecution stepExecution) {
        if (stepExecution == null) {
            return null;
        }
        return new StepExecutionDocument(
                stepExecution.getStepExecutionId(),
                stepExecution.getJobExecutionId(),
                stepExecution.getName(),
                stepExecution.getStatus(),
                stepExecution.getReadCount(),
                stepExecution.getWriteCount(),
                stepExecution.getCommitCount(),
                stepExecution.getRollbackCount(),
                stepExecution.getReadSkipCount(),
                stepExecution.getProcessSkipCount(),
                stepExecution.getWriteSkipCount(),
                stepExecution.getFilterCount(),
                stepExecution.getStartTime(),
                stepExecution.getCreateTime(),
                stepExecution.getEndTime(),
                stepExecution.getLastUpdated(),
                toDocument(stepExecution.getExecutionContext()),
                toDocument(stepExecution.getExitStatus()),
                stepExecution.isTerminateOnly());
    }

    static StepExecution fromDocument(StepExecutionDocument document) {
        if (document == null) {
            return null;
        }
        StepExecution stepExecution = new StepExecution();
        stepExecution.setStepExecutionId(document.stepExecutionId());
        stepExecution.setJobExecutionId(document.jobExecutionId());
        stepExecution.setName(document.name());
        stepExecution.setStatus(document.status());
        stepExecution.setReadCount(document.readCount());
        stepExecution.setWriteCount(document.writeCount());
        stepExecution.setCommitCount(document.commitCount());
        stepExecution.setRollbackCount(document.rollbackCount());
        stepExecution.setReadSkipCount(document.readSkipCount());
        stepExecution.setProcessSkipCount(document.processSkipCount());
        stepExecution.setWriteSkipCount(document.writeSkipCount());
        stepExecution.setFilterCount(document.filterCount());
        stepExecution.setStartTime(document.startTime());
        stepExecution.setCreateTime(document.createTime());
        stepExecution.setEndTime(document.endTime());
        stepExecution.setLastUpdated(document.lastUpdated());
        stepExecution.setExecutionContext(fromDocument(document.executionContext()));
        stepExecution.setExitStatus(fromDocument(document.exitStatus()));
        stepExecution.setTerminateOnly(document.terminateOnly());
        return stepExecution;
    }

    static JobExecutionDocument toDocument(JobExecution jobExecution) {
        return new JobExecutionDocument(
                jobExecution.getJobExecutionId(),
                jobExecution.getJobInstanceId(),
                jobExecution.getJobParameters().stream().map(DocumentMapper::toDocument).collect(Collectors.toSet()),
                jobExecution.getStepExecutions().stream().map(DocumentMapper::toDocument).toList(),
                jobExecution.getStatus(),
                jobExecution.getStartTime(),
                jobExecution.getCreateTime(),
                jobExecution.getEndTime(),
                jobExecution.getLastUpdated(),
                toDocument(jobExecution.getExitStatus()),
                toDocument(jobExecution.getExecutionContext()));
    }

    static JobExecution fromDocument(JobExecutionDocument document) {
        if (document == null) {
            return null;
        }
        JobExecution jobExecution = new JobExecution();
        jobExecution.setJobExecutionId(document.jobExecutionId());
        jobExecution.setJobInstanceId(document.jobInstanceId());
        jobExecution.setJobParameters(document.jobParameters().stream()
                .map(DocumentMapper::fromDocument)
                .collect(Collectors.toSet()));
        jobExecution.setStepExecutions(document.stepExecutions().stream()
                .map(DocumentMapper::fromDocument)
                .toList());
        jobExecution.setStatus(document.status());
        jobExecution.setStartTime(document.startTime());
        jobExecution.setCreateTime(document.createTime());
        jobExecution.setEndTime(document.endTime());
        jobExecution.setLastUpdated(document.lastUpdated());
        jobExecution.setExitStatus(fromDocument(document.exitStatus()));
        jobExecution.setExecutionContext(fromDocument(document.executionContext()));
        return jobExecution;
    }
}
