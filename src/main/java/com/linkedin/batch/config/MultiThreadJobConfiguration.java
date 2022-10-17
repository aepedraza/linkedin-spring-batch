package com.linkedin.batch.config;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.domain.TrackedOrder;
import com.linkedin.batch.reader.OrderRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

@Configuration
public class MultiThreadJobConfiguration {

    private static final int CHUNK_SIZE = 10;
    private static final String INSERT_ORDER_SQL = "insert into "
            + "TRACKED_ORDER(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date, tracking_number, free_shipping)"
            + " values(:orderId,:firstName,:lastName,:email,:itemId,:itemName,:cost,:shipDate,:trackingNumber, :freeShipping)";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource datasource;

    @Bean
    public Job multiThreadJob(Step multiThreadStep) {
        return jobBuilderFactory.get("multiThreadJob")
                .start(multiThreadStep)
                .build();
    }

    @Bean
    public Step multiThreadStep(
            ItemReader<Order> multiThreadPagingJdbcItemReader,
            ItemProcessor<Order, TrackedOrder> freeShippingProcessor,
            ItemWriter<TrackedOrder> beanMappedTrackedOrderItemWriter) {

        return stepBuilderFactory.get("multiThreadStep")
                .<Order, TrackedOrder> chunk(CHUNK_SIZE)
                .reader(multiThreadPagingJdbcItemReader)
                .processor(freeShippingProcessor)
                .writer(beanMappedTrackedOrderItemWriter)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public ItemReader<Order> multiThreadPagingJdbcItemReader(PagingQueryProvider queryProvider) {
        return new JdbcPagingItemReaderBuilder<Order>()
                .dataSource(datasource)
                .name("jdbcPagingItemReader")
                .queryProvider(queryProvider)
                .rowMapper(new OrderRowMapper())
                .pageSize(CHUNK_SIZE)
                .saveState(false) // prevent reader to save state for restarting in a multi-thread scenario
                .build();
    }

    @Bean
    public ItemWriter<TrackedOrder> beanMappedTrackedOrderItemWriter() {
        return new JdbcBatchItemWriterBuilder<TrackedOrder>()
                .dataSource(datasource)
                .sql(INSERT_ORDER_SQL)
                .beanMapped()
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        // lose restart capability due to use of multiple threads
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(CHUNK_SIZE);
        return executor;
    }
}
