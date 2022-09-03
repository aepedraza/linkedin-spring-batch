package com.linkedin.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job deliverPackageJob() {
        return jobBuilderFactory.get("deliverPackageJob")
                .start(packageItemStep())
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
}
