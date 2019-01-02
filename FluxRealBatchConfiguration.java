package com.bpc.ayh.batch;

import javax.persistence.EntityManagerFactory;

import org.hibernate.SessionFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import com.bpc.ayh.batch.classifier.CustomClassifier;
import com.bpc.ayh.batch.listeners.ProcessFluxListener;
import com.bpc.ayh.batch.mappers.FromFluxRealiseFieldSetMapper;
import com.bpc.ayh.batch.processes.FluxRealProcessor;
import com.bpc.ayh.batch.writers.CustomFlatFileItemWriter;
import com.bpc.ayh.batch.writers.CustomHibernateItemWriter;
import com.bpc.ayh.common.dto.FluxRealiseDto;
import com.bpc.ayh.service.BatchTraitementService;
import com.bpc.ayh.service.FluxService;

@Configuration
@EnableBatchProcessing
@EnableConfigurationProperties
@PropertySource("classpath:Referentiels.properties")
public class FluxRealBatchConfiguration extends DefaultBatchConfigurer {
	//
	//
	@Autowired
	EntityManagerFactory entityManagerFactory;

	@Autowired
	PlatformTransactionManager transactionManager;

	@Autowired
	JobRepository jobRepository;

	@Autowired
	JobExplorer jobExplorer;

	@Autowired
	SimpleJobOperator jobOperator;

	@Value("${batch.newMatiass.fluxpav.in.attributes}")
	private String[] fluxPavInAttributes;

	/* creer property point virgule */
	@Value("${batch.newMatiass.batch.in.out.delimiter}")
	private String delimiter;

	@Value("${batch.newMatiass.fluxreal.out.attributes}")
	private String[] names;

	@Value("${batch.newmatiass.fluxreal.type}")
	private String fluxType;

	@Value("${batch.data.dateFormat}")
	private String dataDateFormat;

	/* Définition des bean des mappers */
	@Bean
	public LineMapper<FluxRealiseDto> fluxRealiseLineMapper() {
		// lineMapper
		DefaultLineMapper<FluxRealiseDto> lineMapper = new DefaultLineMapper<FluxRealiseDto>();
		// lineTokenizer
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames(fluxPavInAttributes);
		lineTokenizer.setDelimiter(delimiter);
		lineMapper.setLineTokenizer(lineTokenizer);
		// fieldSetMapper
		FromFluxRealiseFieldSetMapper fieldSetMapper = new FromFluxRealiseFieldSetMapper();
		fieldSetMapper.setDataDateFormat(dataDateFormat);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}

	/* Récupèration du nom du fichier depuis lejobParameters */
	@Bean
	@StepScope
	public Resource fluxRealCsvIn(@Value("${batch.path.in}/#{jobParameters[fileName]}") String fileName) {
		return new FileSystemResource(fileName);
	}

	// tag::readerwriterprocessor[]
	@Bean
	@StepScope
	public FlatFileItemReader<FluxRealiseDto> fluxRealReader(LineMapper<FluxRealiseDto> lineMapper) {
		FlatFileItemReader<FluxRealiseDto> csvfluxRealFileReader = new FlatFileItemReader<>();
		csvfluxRealFileReader.setName("flatItemReader");
		String fluxCsvInPathFile = "";
		csvfluxRealFileReader.setResource(fluxRealCsvIn(fluxCsvInPathFile));
		csvfluxRealFileReader.setLineMapper(lineMapper);
		return csvfluxRealFileReader;
	}

	@Bean
	public FluxRealProcessor fluxRealProcessor() {
		return new FluxRealProcessor();
	}

	@Bean(name = "fluxRealiseClassifier")
	public CustomClassifier<FluxRealiseDto> fluxRealiseClassifier(
			CustomFlatFileItemWriter<FluxRealiseDto> customFlatFileItemWriter,
			CustomHibernateItemWriter<FluxRealiseDto> oracleFluxRealiseItemWriter) {
		CustomClassifier<FluxRealiseDto> classifier = new CustomClassifier<FluxRealiseDto>();
		classifier.setJdbcItemWriter(oracleFluxRealiseItemWriter);
		classifier.setFileItemWriter(customFlatFileItemWriter);
		return classifier;
	}

	@Bean(name = "classifierFluxRealWriter")
	public ClassifierCompositeItemWriter<FluxRealiseDto> fluxRealwriter(
			Classifier<FluxRealiseDto, ItemWriter<? super FluxRealiseDto>> fluxRealiseClassifier) {
		ClassifierCompositeItemWriter<FluxRealiseDto> fluxRealiseWriterClassifier = new ClassifierCompositeItemWriter<FluxRealiseDto>();
		fluxRealiseWriterClassifier.setClassifier(fluxRealiseClassifier);
		return fluxRealiseWriterClassifier;
	}

	@Bean(name = "oracleFluxRealiseItemWriter")
	public CustomHibernateItemWriter<FluxRealiseDto> oracleFluxRealiseItemWriter(FluxService fluxService) {
		CustomHibernateItemWriter<FluxRealiseDto> writer = new CustomHibernateItemWriter<FluxRealiseDto>();
		SessionFactory sessionFactory = entityManagerFactory.unwrap(SessionFactory.class);
		writer.setFluxService(fluxService);
		writer.setSessionFactory(sessionFactory);
		writer.setTypeFlux("REAL");
		return writer;
	}

	/* du nom du fichier e sortie dpeuis le fichier d'entrée */

	@Bean
	@StepScope
	public Resource fluxRealOut(@Value("${batch.path.out}/#{jobParameters[fileName]}") String fileName) {
		return new FileSystemResource(fileName.replace(".csv", "_out.csv"));
	}

	@Bean(name = "customRealflatFileItemWriter")
	public CustomFlatFileItemWriter<FluxRealiseDto> CustomFlatFileItemWriter(
			BatchTraitementService batchTraitementService) {
		CustomFlatFileItemWriter<FluxRealiseDto> writer = new CustomFlatFileItemWriter<FluxRealiseDto>();
		writer.setCompteRenduService(batchTraitementService);
		String fluxOutPathFile = "";
		writer.setResource(fluxRealOut(fluxOutPathFile));
		writer.setAppendAllowed(true);
		writer.setShouldDeleteIfEmpty(true);
		DelimitedLineAggregator<FluxRealiseDto> aggregator = new DelimitedLineAggregator<FluxRealiseDto>();
		aggregator.setDelimiter(delimiter);
		BeanWrapperFieldExtractor<FluxRealiseDto> extractor = new BeanWrapperFieldExtractor<FluxRealiseDto>();
		extractor.setNames(names);
		aggregator.setFieldExtractor(extractor);
		writer.setLineAggregator(aggregator);
		return writer;
	}

	@Autowired
	CustomFlatFileItemWriter<FluxRealiseDto> customFluxRealflatFileItemWriter;

	// tag::jobstep[]

	@Bean
	public Step stepFluxReal(FlatFileItemReader<FluxRealiseDto> reader,
			ClassifierCompositeItemWriter<FluxRealiseDto> writer,
			// TaskExecutor taskExecutor,
			FluxRealProcessor processor) {

		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);

		return stepBuilderFactory.get("stepFluxReal").<FluxRealiseDto, FluxRealiseDto>chunk(10).reader(reader)
				.faultTolerant().skip(FlatFileParseException.class).skipLimit(1000).processor(processor).writer(writer)
				// .taskExecutor(taskExecutor)
				.stream(customFluxRealflatFileItemWriter).build();
	}

	@Bean(name = "fluxRealiseJob")
	public Job fluxrealiseJob(JobRepository jobRepository, ProcessFluxListener processListener, Step stepFluxReal) {
		JobBuilderFactory jobBuilderFactory = new JobBuilderFactory(jobRepository);
		return jobBuilderFactory.get("fluxRealiseJob").repository(jobRepository).incrementer(new RunIdIncrementer())
				.listener(processListener).flow(stepFluxReal).end().build();
	}

}