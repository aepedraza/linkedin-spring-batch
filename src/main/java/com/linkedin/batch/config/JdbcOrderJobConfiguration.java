package com.linkedin.batch.config;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.reader.OrderRowMapper;
import com.linkedin.batch.writer.OrderItemPreparedStatementSetter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class JdbcOrderJobConfiguration {

    private static final String SELECT_CLAUSE = "SELECT order_id, first_name, last_name, email, cost, item_id, item_name, ship_date";
    private static final String FROM_CLAUSE = "FROM SHIPPED_ORDER";

    // It is recommended to have ORDER BY to ensure order of result set, in case we need to restart/resume the job
    private static final String ORDER_SQL = String.join(" ", SELECT_CLAUSE, FROM_CLAUSE, "ORDER BY order_id");

    public static String PREPARED_STATEMENT_INSERT_ORDER_SQL = "INSERT INTO "
            + "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date) "
            + "VALUES(?,?,?,?,?,?,?,?)";

    // Must match with the POJO fields names
    public static String NAMED_PARAMETERS_INSERT_ORDER_SQL = "INSERT INTO "
            + "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date) "
            + "VALUES(:orderId, :firstName, :lastName, :email, :itemId, :itemName, :cost, :shipDate)";

    private static final int PAGE_CHUNK_SIZE = 10;

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
        return buildStep("jdbcOrderProcessingStep", jdbcItemReader(), preparedStatementWriter());
    }

    private TaskletStep buildStep(String stepName, ItemReader<Order> reader, ItemWriter<Order> writer) {
        return stepBuilderFactory.get(stepName)
                .<Order, Order>chunk(PAGE_CHUNK_SIZE)
                .reader(reader)
                .processor(orderValidatingItemProcessor())
                .writer(writer)
                .build();
    }

    @Bean
    public ItemProcessor<Order, Order> orderValidatingItemProcessor() {
        BeanValidatingItemProcessor<Order> itemProcessor = new BeanValidatingItemProcessor<>();
        itemProcessor.setFilter(true);
        return itemProcessor;
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
    public ItemWriter<Order> preparedStatementWriter() {
        return new JdbcBatchItemWriterBuilder<Order>()
                .dataSource(datasource)
                .sql(PREPARED_STATEMENT_INSERT_ORDER_SQL)
                .itemPreparedStatementSetter(new OrderItemPreparedStatementSetter())
                .build();
    }

    @Bean
    public Job pagingJdbcOrderProcessingJob() throws Exception {
        return buildJob("pagingJdbcOrderProcessingJob", pagingJdbcOrderProcessingStep());
    }

    @Bean
    public Step pagingJdbcOrderProcessingStep() throws Exception {
        return buildStep("pagingJdbcOrderProcessingStep", pagingJdbcItemReader(), beanMappedItemWriter());
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

    @Bean
    public ItemWriter<Order> beanMappedItemWriter() {
        return new JdbcBatchItemWriterBuilder<Order>()
                .dataSource(datasource)
                .sql(NAMED_PARAMETERS_INSERT_ORDER_SQL)
                .beanMapped()
                .build();
    }
}
