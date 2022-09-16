package com.linkedin.batch.config;

import com.linkedin.batch.decider.CorrectItemDecider;
import com.linkedin.batch.decider.DeliveryDecider;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Random;

@Configuration
public class DeliverPackageJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job deliverPackageJob(Flow deliveryFlow, Step nestedBillingJobStep) {
        return jobBuilderFactory.get("deliverPackageJob")
                .start(packageItemStep())
                .on("*").to(deliveryFlow)
                .next(nestedBillingJobStep)
                .end()
                .build();
    }

    @Bean
    public Step packageItemStep() {
        return stepBuilderFactory.get("packageItemStep")
                .tasklet((contribution, chunkContext) -> {
                    String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
                    String runDate = chunkContext.getStepContext().getJobParameters().get("run.date").toString();
                    System.out.printf("The %s has been packaged at %s\n", item, runDate);
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step nestedBillingJobStep(Job billingJob) {
        return stepBuilderFactory.get("nestedBillingJobStep")
                .job(billingJob)
                .build();
    }
}
