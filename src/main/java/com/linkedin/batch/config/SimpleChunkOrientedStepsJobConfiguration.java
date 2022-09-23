package com.linkedin.batch.config;

import com.linkedin.batch.reader.SimpleItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SimpleChunkOrientedStepsJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job simpleChunkJob() {
        return jobBuilderFactory.get("simpleChunkJob")
                .start(chunkBasedStep())
                .build();
    }

    @Bean
    public Step chunkBasedStep() {
        return stepBuilderFactory.get("chunkBasedStep")
                .<String, String>chunk(3)
                .reader(itemReader())
                .writer(items -> {
                    System.out.printf("Received a list of  size: %d%n", items.size());
                    items.forEach(System.out::println);
                }).build();
    }

    @Bean
    public ItemReader<String> itemReader() {
        return new SimpleItemReader();
    }
}
