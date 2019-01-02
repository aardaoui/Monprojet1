package com.bpc.ayh.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@PropertySource("classpath:application.properties")
public class JobContextConfiguration implements BatchConfigurer {

	PlatformTransactionManager transactionManager;
	JobRepository jobRepository;
	JobExplorer jobExplorer;

	@Value("${batch.databalse.system}")
	private String dataBaSystem;

	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	DataSource dataSource;

	@Override
	public PlatformTransactionManager getTransactionManager() {
		return transactionManager;
	}

	@Bean
	public JobExplorer jobExplorer(DataSource dataSource) throws Exception {
		JobExplorerFactoryBean factory = new JobExplorerFactoryBean();
		factory.setDataSource(dataSource);
		factory.setJdbcOperations(new JdbcTemplate(dataSource));
		factory.afterPropertiesSet();
		this.jobExplorer = factory.getObject();
		return this.jobExplorer;
	}

	/**
	 * All injected dependencies for this bean are provided by
	 * the @EnableBatchProcessing infrastructure out of the box.
	 */
	@Bean
	public SimpleJobOperator jobOperator(JobExplorer jobExplorer, JobRepository jobRepository,
			JobRegistry jobRegistry) {

		SimpleJobOperator jobOperator = new SimpleJobOperator();

		jobOperator.setJobExplorer(jobExplorer);
		jobOperator.setJobRepository(jobRepository);
		jobOperator.setJobRegistry(jobRegistry);
		jobOperator.setJobLauncher(jobLauncher);

		return jobOperator;
	}

	@Bean
	public JobLauncher jobLauncher(JobRepository jobRepository) throws Exception {
		this.jobLauncher = new SimpleJobLauncher();
		((SimpleJobLauncher) this.jobLauncher).setJobRepository(jobRepository);
		return this.jobLauncher;
	}

	@Bean
	public JobRepository jobRepository(DataSource datasource, PlatformTransactionManager transactionManager)
			throws Exception {
		JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
		factory.setDataSource(datasource);
		factory.setTransactionManager(transactionManager);
		factory.setDatabaseType(dataBaSystem);
		this.jobRepository = factory.getObject();
		return jobRepository;
	}

	@Bean
	public JobRegistry jobRegistry() throws Exception {
		MapJobRegistry mapJobRegistry = new MapJobRegistry();
		return mapJobRegistry;
	}

	@Bean
	public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) throws Exception {
		JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
		jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
		return jobRegistryBeanPostProcessor;
	}

	@Override
	public JobRepository getJobRepository() throws Exception {
		return this.jobRepository;
	}

	@Override
	public JobLauncher getJobLauncher() throws Exception {
		return this.jobLauncher;
	}

	@Override
	public JobExplorer getJobExplorer() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
