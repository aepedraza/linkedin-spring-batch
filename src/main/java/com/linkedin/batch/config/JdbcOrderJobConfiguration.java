package com.linkedin.batch.config;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.reader.OrderRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;

@Configuration
public class JdbcOrderJobConfiguration {

    private static final String SELECT_CLAUSE = "SELECT order_id, first_name, last_name, email, cost, item_id, item_name, ship_date";
    private static final String FROM_CLAUSE = "FROM SHIPPED_ORDER";

    // It is recommended to have ORDER BY to ensure order of result set, in case we need to restart/resume the job
    private static final String ORDER_SQL = String.join(" ", SELECT_CLAUSE, FROM_CLAUSE, "ORDER BY order_id");

    private static final int PAGE_CHUNK_SIZE = 10;

    private static final String[] NAMES = new String[]{"orderId", "firstName", "lastName", "email", "cost", "itemId",
            "itemName", "shipDate"};

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource datasource;

    @Bean
    public Job jdbcOrderProcessingJob() {
        return buildJob("jdbcOrderProcessingJob", jdbcOrderProcessingStep());
    }

    private Job buildJob(String jobName, Step step) {
        return jobBuilderFactory.get(jobName)
                .start(step)
                .build();
    }

    @Bean
    public Step jdbcOrderProcessingStep() {
        return buildStep("jdbcOrderProcessingStep", jdbcItemReader());
    }

    private TaskletStep buildStep(String stepName, ItemReader<Order> reader) {
        return stepBuilderFactory.get(stepName)
                .<Order, Order>chunk(PAGE_CHUNK_SIZE)
                .reader(reader)
                .writer(csvItemWriter())
                .build();
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

    @Bean
    public ItemReader<Order> jdbcItemReader() {
        return new JdbcCursorItemReaderBuilder<Order>()
                .dataSource(datasource)
                .name("jdbcCursorItemReader")
                .sql(ORDER_SQL)
                .rowMapper(new OrderRowMapper())
                .build();
    }

    @Bean
    public Job pagingJdbcOrderProcessingJob() throws Exception {
        return buildJob("pagingJdbcOrderProcessingJob", pagingJdbcOrderProcessingStep());
    }

    @Bean
    public Step pagingJdbcOrderProcessingStep() throws Exception {
        return buildStep("pagingJdbcOrderProcessingStep", pagingJdbcItemReader());
    }

    @Bean
    public ItemReader<Order> pagingJdbcItemReader() throws Exception {
        return new JdbcPagingItemReaderBuilder<Order>()
                .dataSource(datasource)
                .name("jdbcPagingItemReader")
                .queryProvider(queryProvider())
                .rowMapper(new OrderRowMapper())
                .pageSize(PAGE_CHUNK_SIZE) // must be the same as the chunk size
                .build();
    }

    @Bean
    public PagingQueryProvider queryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();

        factory.setSelectClause(SELECT_CLAUSE);
        factory.setFromClause(FROM_CLAUSE);
        factory.setSortKey("order_id");
        factory.setDataSource(datasource);

        // Will construct appropriate paging query depending on the underlying database
        return factory.getObject();
    }
}
