package com.linkedin.batch.config;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.reader.OrderRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class JdbcOrderJobConfiguration {

    // It is recommended to have ORDER BY to ensure order of result set, in case we need to restart/resume the job
    private static final String ORDER_SQL = "SELECT order_id, first_name, last_name, " +
            "email, cost, item_id, item_name, ship_date " +
            "FROM SHIPPED_ORDER " +
            "ORDER BY order_id";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource datasource;

    @Bean
    public Job jdbcOrderProcessingJob() {
        return jobBuilderFactory.get("jdbcOrderProcessingJob")
                .start(jdbcOrderProcessingStep())
                .build();
    }

    @Bean
    public Step jdbcOrderProcessingStep() {
        return stepBuilderFactory.get("jdbcOrderProcessingStep")
                .<Order, Order>chunk(3)
                .reader(jdbcItemReader())
                .writer(items -> {
                    System.out.printf("Received a list of  size: %d%n", items.size());
                    items.forEach(System.out::println);
                }).build();
    }

    @Bean
    public ItemReader<Order> jdbcItemReader() {
        return new JdbcCursorItemReaderBuilder<Order>()
                .dataSource(datasource)
                .name("jdbcCursorItemReader")
                .sql(ORDER_SQL)
                .rowMapper(new OrderRowMapper())
                .build();
    }
}
