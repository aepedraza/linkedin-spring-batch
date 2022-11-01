package com.linkedin.batch.config;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.time.LocalDateTime;

@Configuration
@EnableScheduling
public class QuartzScheduledJob extends QuartzJobBean {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobExplorer jobExplorer;

    @Bean
    public JobDetail jobDetail() {
        return JobBuilder.newJob(QuartzScheduledJob.class) // Job for this class
                .storeDurably() // info about job is retained
                .build();
    }

    @Bean
    public Trigger trigger() { // Determines when to execute the job
        SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder
                .simpleSchedule()
                .withIntervalInSeconds(30)
                .repeatForever();

        return TriggerBuilder.newTrigger()
                .forJob(jobDetail())
                .withSchedule(scheduleBuilder)
                .build();
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        JobParameters jobParameters = new JobParametersBuilder(jobExplorer)
                .getNextJobParameters(getQuartzRunTimeJob())
                .toJobParameters();

        try {
            jobLauncher.run(getQuartzRunTimeJob(), jobParameters);
        } catch (JobExecutionAlreadyRunningException | JobRestartException |
                 JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
            e.printStackTrace();
        }
    }

    @Bean
    public Job getQuartzRunTimeJob() {
        return jobBuilderFactory.get("getQuartzRunTimeJob")
                .incrementer(new RunIdIncrementer())
                .start(getRunTimeStep())
                .build();
    }

    private Step getRunTimeStep() {
        return stepBuilderFactory.get("getRunTimeStep")
                .tasklet(getRunTimeTasklet())
                .build();
    }

    private Tasklet getRunTimeTasklet() {
        return (contribution, chunkContext) -> {
            System.out.println("The run time is: " + LocalDateTime.now());
            return RepeatStatus.FINISHED;
        };
    }
}
