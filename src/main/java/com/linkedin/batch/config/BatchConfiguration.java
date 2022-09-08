package com.linkedin.batch.config;

import com.linkedin.batch.decider.CorrectItemDecider;
import com.linkedin.batch.decider.DeliveryDecider;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
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
                    .on("FAILED").to(storePackageStep())
                .from(driveToAddressStep())
                    .on("*").to(deliveryDecider())
                        .on("PRESENT")
                            .to(givePackageToCustomerStep())
                            .next(correctItemDecider())
                                .on("CORRECT").to(thankCustomerStep())
                            .from(correctItemDecider())
                                .on("INCORRECT").to(giveRefundStep())
                    .from(deliveryDecider())
                        .on("NOT_PRESENT").to(leaveAtDoorStep())
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
    public Step storePackageStep() {
        return stepBuilderFactory.get("storePackageStep")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Storing the package while the customer address is located.");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public JobExecutionDecider deliveryDecider() {
        return new DeliveryDecider();
    }

    @Bean
    public Step leaveAtDoorStep() {
        return stepBuilderFactory.get("leaveAtDoorStep")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Leaving the package at te door.");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step givePackageToCustomerStep() {
        return stepBuilderFactory.get("givePackageToCustomerStep")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Given the package to the customer");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public JobExecutionDecider correctItemDecider() {
        return new CorrectItemDecider();
    }

    @Bean
    public Step thankCustomerStep() {
        return stepBuilderFactory.get("thankCustomerStep")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Thank you for your purchase!");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step giveRefundStep() {
        return stepBuilderFactory.get("giveRefundStep")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Sorry for the mistake, You will be refunded.");
                    return RepeatStatus.FINISHED;
                }).build();
    }


}
