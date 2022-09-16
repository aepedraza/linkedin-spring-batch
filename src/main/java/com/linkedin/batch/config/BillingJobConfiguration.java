package com.linkedin.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BillingJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job billingJob() {
        return jobBuilderFactory.get("billingJob")
                .start(billingStep())
                .build();
    }

    @Bean
    public Step billingStep() {
        return stepBuilderFactory.get("billingStep")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Sending billing invoice to customer");
                    return RepeatStatus.FINISHED;
                }).build();
    }

}
