package com.linkedin.batch.config;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.domain.TrackedOrder;
import com.linkedin.batch.exception.OrderProcessingException;
import com.linkedin.batch.listener.CustomRetryListener;
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
public class RetryLogicJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job retryJob(Step retryStep) {
        return jobBuilderFactory.get("retryJob")
                .start(retryStep)
                .build();
    }

    @Bean
    public Step retryStep(
            ItemReader<Order> pagingJdbcItemReader,
            ItemProcessor<Order, TrackedOrder> freeShippingProcessor,
            ItemWriter<TrackedOrder> jsonWriter) {

        return stepBuilderFactory.get("retryStep")
                .<Order, TrackedOrder>chunk(10)
                .reader(pagingJdbcItemReader)
                .processor(freeShippingProcessor)
                .faultTolerant()
                .retry(OrderProcessingException.class)
                .retryLimit(3) // indicates retries for item
                .listener(new CustomRetryListener())
                .writer(jsonWriter)
                .build();
    }
}
