package com.linkedin.batch.decider;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

import java.util.Random;

public class CorrectItemDecider implements JobExecutionDecider {

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        boolean isCorrect = new Random().nextDouble() < 0.7;
        String result = isCorrect ? "CORRECT" : "INCORRECT";
        System.out.println("Delivered item is " + result);
        return new FlowExecutionStatus(result);
    }
}
