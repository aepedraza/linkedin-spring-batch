package com.linkedin.batch.config;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.domain.TrackedOrder;
import com.linkedin.batch.exception.OrderProcessingException;
import com.linkedin.batch.listener.CustomSkipListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SkipLogicJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job skipJob(Step skipStep) {
        return jobBuilderFactory.get("skipJob")
                .start(skipStep)
                .build();
    }

    @Bean
    public Step skipStep(
            ItemReader<Order> pagingJdbcItemReader,
            ItemProcessor<Order, TrackedOrder> freeShippingProcessor,
            ItemWriter<TrackedOrder> jsonWriter) {

        return stepBuilderFactory.get("skipStep")
                .<Order, TrackedOrder>chunk(10)
                .reader(pagingJdbcItemReader)
                .processor(freeShippingProcessor)
                .faultTolerant()
                .skip(OrderProcessingException.class)
                .skipLimit(5) // indicates skip limit for entire step
                .listener(new CustomSkipListener())
                .writer(jsonWriter)
                .build();
    }
}
