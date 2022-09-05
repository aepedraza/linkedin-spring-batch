package com.linkedin.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Random;

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
                .next(driveToAddressStep())
                .next(givePackageToCustomerStep())
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
    public Step driveToAddressStep() {
        return stepBuilderFactory.get("driveToAddressStep")
                .tasklet((contribution, chunkContext) -> {

                    if (gotLost(chunkContext)) {
                        throw new RuntimeException("Got lost driving to the address");
                    }


                    System.out.println("Successfully arrived at the address");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    private Boolean gotLost(ChunkContext chunkContext) {
        Double gotLostProbability = (Double) chunkContext.getStepContext().getJobParameters().get("gotLostProbability");
        double gotLost = new Random().nextDouble();
        return gotLost < gotLostProbability;
    }

    @Bean
    public Step givePackageToCustomerStep() {
        return stepBuilderFactory.get("givePackageToCustomerStep")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Given the package to the customer");
                    return RepeatStatus.FINISHED;
                }).build();
    }


}
