package com.linkedin.batch.config;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.reader.OrderFieldSetMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
public class OrderJobConfiguration {

    private static final String[] TOKENS = new String[]{
            "order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"};
    private static final String[] NAMES = new String[]{
            "orderId", "firstName", "lastName", "email", "cost", "itemId", "itemName", "shipDate"};

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job orderProcessingJob() {
        return jobBuilderFactory.get("orderProcessingJob")
                .start(orderProcessingStep())
                .build();
    }

    @Bean
    public Step orderProcessingStep() {
        return stepBuilderFactory.get("orderProcessingStep")
                .<Order, Order>chunk(3)
                .reader(csvItemReader())
                .writer(csvItemWriter())
                .build();
    }

    @Bean
    public ItemReader<Order> csvItemReader() {
        FlatFileItemReader<Order> itemReader = new FlatFileItemReader<>();
        itemReader.setLinesToSkip(1); // skip headers
        itemReader.setResource(new FileSystemResource("../data/shipped_orders.csv"));

        DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(TOKENS);
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new OrderFieldSetMapper());

        itemReader.setLineMapper(lineMapper);

        return itemReader;
    }

    @Bean
    public ItemWriter<Order> csvItemWriter() {
        FlatFileItemWriter<Order> itemWriter = new FlatFileItemWriter<>();

        itemWriter.setResource(new FileSystemResource("./data/shipped_orders_output.csv"));

        DelimitedLineAggregator<Order> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");

        BeanWrapperFieldExtractor<Order> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(NAMES);
        aggregator.setFieldExtractor(fieldExtractor);

        itemWriter.setLineAggregator(aggregator);
        return itemWriter;
    }
}
