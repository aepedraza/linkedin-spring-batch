package com.linkedin.batch.config;

import com.linkedin.batch.decider.CorrectItemDecider;
import com.linkedin.batch.decider.DeliveryDecider;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Random;

@Configuration
public class CommonFlowsConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Flow deliveryFlow() {
        return new FlowBuilder<SimpleFlow>("deliveryFlow")
                .start(driveToAddressStep())
                    .on("FAILED").fail()
                .from(driveToAddressStep())
                    .on("*").to(deliveryDecider())
                        .on("PRESENT").to(givePackageToCustomerStep())
                            .next(correctItemDecider()).on("CORRECT").to(thankCustomerStep())
                            .from(correctItemDecider()).on("INCORRECT").to(giveRefundStep())
                    .from(deliveryDecider())
                        .on("NOT_PRESENT").to(leaveAtDoorStep())
                .build();
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

    @Bean
    public Flow billingFlow() {
        return new FlowBuilder<SimpleFlow>("billingFlow")
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
