package com.linkedin.batch.config;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.domain.TrackedOrder;
import com.linkedin.batch.processor.TrackedOrderItemProcessor;
import com.linkedin.batch.reader.OrderRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;

@Configuration
public class JsonOutputJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource datasource;

    @Bean
    public Job dbToJsonJob(Step dbToJsonStep) {
        return jobBuilderFactory.get("dbToJsonJob")
                .start(dbToJsonStep)
                .build();
    }

    @Bean
    public Step dbToJsonStep(ItemReader<Order> jdbcItemReader) {
        return stepBuilderFactory.get("dbToJsonStep")
                .<Order, TrackedOrder>chunk(100)
                .reader(jdbcItemReader)
                .processor(trackedOrderItemProcessor())
                .writer(jsonWriter())
                .build();

    }

    @Bean
    public ItemProcessor<Order, TrackedOrder> trackedOrderItemProcessor() {
        return new TrackedOrderItemProcessor();
    }

    @Bean
    public ItemWriter<TrackedOrder> jsonWriter() {
        return new JsonFileItemWriterBuilder<TrackedOrder>()
                .name("jsonWriter")
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                .resource(new FileSystemResource("./data/shipped_orders_output.json"))
                .build();
    }
}
