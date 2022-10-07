package com.linkedin.batch.config;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.domain.TrackedOrder;
import com.linkedin.batch.processor.FreeShippingItemProcessor;
import com.linkedin.batch.processor.TrackedOrderItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Arrays;

@Configuration
public class FreeShippingJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource datasource;

    @Bean
    public Job freeShippingJob(Step freeShippingStep) {
        return jobBuilderFactory.get("freeShippingJob")
                .start(freeShippingStep)
                .build();
    }

    @Bean
    public Step freeShippingStep(ItemReader<Order> jdbcItemReader, ItemWriter<TrackedOrder> jsonWriter) {
        return stepBuilderFactory.get("freeShippingStep")
                .<Order, TrackedOrder>chunk(100)
                .reader(jdbcItemReader)
                .processor(freeShippingProcessor())
                .writer(jsonWriter)
                .build();
    }

    private ItemProcessor<Order, TrackedOrder> freeShippingProcessor() {
        CompositeItemProcessor<Order, TrackedOrder> compositeItemProcessor = new CompositeItemProcessor<>();
        compositeItemProcessor.setDelegates(Arrays.asList(
                new TrackedOrderItemProcessor(),
                new FreeShippingItemProcessor()
        ));

        return compositeItemProcessor;
    }
}
