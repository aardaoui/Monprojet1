package com.bpc.ayh.batch;

import java.util.HashMap;
import java.util.Map;

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
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.transaction.PlatformTransactionManager;

import com.bpc.ayh.batch.classifier.CustomFluxFraisGestionClassifier;
import com.bpc.ayh.batch.listeners.ProcessFluxListener;
import com.bpc.ayh.batch.mappers.FromFluxFraisGestionFieldSetMapper;
import com.bpc.ayh.batch.processes.FraisGestionStep1Processor;
import com.bpc.ayh.batch.processes.FraisGestionStep2Processor;
import com.bpc.ayh.batch.readers.CustomHibernateItemReader;
import com.bpc.ayh.batch.readers.CustomRepositoryItemReader;
import com.bpc.ayh.batch.writers.CustomFlatFileItemWriter;
import com.bpc.ayh.batch.writers.CustomHibernateItemWriter;
import com.bpc.ayh.cmn.entity.TmpAgregFraisGest;
import com.bpc.ayh.cmn.entity.TmpAgregFraisGestVue;
import com.bpc.ayh.cmn.rep.FluxOutTempVueRep;
import com.bpc.ayh.common.dto.FraisGestionDto;
import com.bpc.ayh.common.dto.TmpAgregFraisGestVueDto;
import com.bpc.ayh.common.util.CachableInfo;
import com.bpc.ayh.service.BatchTraitementService;
import com.bpc.ayh.service.FluxService;

@Configuration
@EnableBatchProcessing
@EnableConfigurationProperties
public class FraisGestionBatchConfiguration extends DefaultBatchConfigurer {
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

	/* property point virgule*/
	@Value("${batch.newMatiass.batch.in.out.delimiter}")
	private String delimiter;

	@Value("${batch.newMatiass.fluxfraisgestion.out.attributes}")
	private String[] names;

	@Value("${batch.newmatiass.fluxfraisgestion.type}")
	private String fluxType;

	@Value("${batch.data.dateFormat}")
	private String dataDateFormat;

	/* Définition des bean des mappers */
	@Bean
	public LineMapper<FraisGestionDto> fraisGestionLineMapper() {
		// lineMapper
		DefaultLineMapper<FraisGestionDto> lineMapper = new DefaultLineMapper<FraisGestionDto>();
		// lineTokenizer
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames(fluxPavInAttributes);
		lineTokenizer.setDelimiter(delimiter);
		lineMapper.setLineTokenizer(lineTokenizer);
		// fieldSetMapper
		FromFluxFraisGestionFieldSetMapper fieldSetMapper = new FromFluxFraisGestionFieldSetMapper();
		fieldSetMapper.setDataDateFormat(dataDateFormat);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}

	/* Récupèration du nom du fichier depuis lejobParameters */
	@Bean
	@StepScope
	public Resource fraisGestionCsvIn(@Value("${batch.path.in}/#{jobParameters[fileName]}") String fileName) {
		return new FileSystemResource(fileName);
	}

	// tag::readerwriterprocessor[]
	@Bean
	@StepScope
	public FlatFileItemReader<FraisGestionDto> fraisGestionReader(LineMapper<FraisGestionDto> lineMapper) {
		String fluxCsvInPathFile = "";
		FlatFileItemReader<FraisGestionDto> csvfraisGestionFileReader = new FlatFileItemReader<>();
		
		csvfraisGestionFileReader.setName("fraisGestionFlatItemReader");
		csvfraisGestionFileReader.setResource(fraisGestionCsvIn(fluxCsvInPathFile));
		csvfraisGestionFileReader.setLineMapper(lineMapper);
		return csvfraisGestionFileReader;
	}

	@Bean
	public FraisGestionStep1Processor fraisGestionProcessor() {
		FraisGestionStep1Processor fraisGestionStep1Processor = new FraisGestionStep1Processor();
		fraisGestionStep1Processor.setJobExplorer(jobExplorer);
		fraisGestionStep1Processor.setDataDateFormat(dataDateFormat);		
		fraisGestionStep1Processor.setJobOperator(jobOperator);
		return fraisGestionStep1Processor;
	}

	@Bean
	public FraisGestionStep2Processor<TmpAgregFraisGestVue, TmpAgregFraisGestVueDto> FraisGestionStep2Processor() {
		return new FraisGestionStep2Processor<TmpAgregFraisGestVue, TmpAgregFraisGestVueDto>();
	}

	@Bean(name = "fraisGestionClassifier")
	public CustomFluxFraisGestionClassifier fraisGestionClassifier(
			CustomHibernateItemWriter<FraisGestionDto> oracleFraisGestioniseItemWriter) {
		CustomFluxFraisGestionClassifier classifier = new CustomFluxFraisGestionClassifier();
		classifier.setJdbcItemWriter(oracleFraisGestioniseItemWriter);
		return classifier;
	}

	@Bean(name = "classifierFraisGestionWriter")
	public ClassifierCompositeItemWriter<FraisGestionDto> fraisGestionwriter(
			Classifier<FraisGestionDto, ItemWriter<? super FraisGestionDto>> fraisGestionClassifier) {
		ClassifierCompositeItemWriter<FraisGestionDto> classifier = new ClassifierCompositeItemWriter<FraisGestionDto>();
		classifier.setClassifier(fraisGestionClassifier);
		return classifier;
	}

	@Bean(name = "oracleFraisGestionItemWriter")
	public CustomHibernateItemWriter<FraisGestionDto> oracleFraisGestioniseItemWriter(FluxService fluxService) {
		CustomHibernateItemWriter<FraisGestionDto> writer = new CustomHibernateItemWriter<FraisGestionDto>();
		SessionFactory sessionFactory = entityManagerFactory.unwrap(SessionFactory.class);
		writer.setFluxService(fluxService);
		writer.setSessionFactory(sessionFactory);
		writer.setTypeFlux("FRAISG");
		return writer;
	}

	@Bean(name = "oracleFraisGestionItemReader")
	public CustomHibernateItemReader<TmpAgregFraisGest> oracleFraisGestionItemReader(FluxService fluxService) {
		CustomHibernateItemReader<TmpAgregFraisGest> reader = new CustomHibernateItemReader<TmpAgregFraisGest>();
		SessionFactory sessionFactory = entityManagerFactory.unwrap(SessionFactory.class);
		reader.setQueryString("from TmpAgregFraisGest");
		reader.setSessionFactory(sessionFactory);
		return reader;
	}

	/* du nom du fichier e sortie dpeuis le fichier d'entrée */
	@Bean
	@StepScope
	public Resource fraisGestionOut(@Value("${batch.path.out}/#{jobParameters[fileName]}") String fileName) {
		return new FileSystemResource(fileName.replace(".csv", "_out.csv"));
	}

	@Bean(name = "customFraisGestionflatFileItemWriter")
	@StepScope
	public CustomFlatFileItemWriter<TmpAgregFraisGestVueDto> customFraisGestionflatFileItemWriter(
			BatchTraitementService batchTraitementService) {
		String fluxOutPathFile = "";
		CustomFlatFileItemWriter<TmpAgregFraisGestVueDto> writer = new CustomFlatFileItemWriter<>();
		writer.setCompteRenduService(batchTraitementService);
		writer.setResource(fraisGestionOut(fluxOutPathFile));
		writer.setShouldDeleteIfExists(true);
		writer.setAppendAllowed(true);
		writer.setShouldDeleteIfEmpty(true);
		writer.setShouldDeleteIfExists(true);
		DelimitedLineAggregator<TmpAgregFraisGestVueDto> aggregator = new DelimitedLineAggregator<>();
		aggregator.setDelimiter(delimiter);
		BeanWrapperFieldExtractor<TmpAgregFraisGestVueDto> extractor = new BeanWrapperFieldExtractor<>();
		extractor.setNames(names);
		aggregator.setFieldExtractor(extractor);
		writer.setLineAggregator(aggregator);
		return writer;
	}

	@Autowired
	private FluxOutTempVueRep fluxOutTempVueRep;

	@Autowired
	private CustomFlatFileItemWriter<TmpAgregFraisGestVueDto> customFraisGestionVueflatFileItemWriter;

	@Bean
	public Step stepFraisGestion_1(FlatFileItemReader<FraisGestionDto> reader,
			ClassifierCompositeItemWriter<FraisGestionDto> writer, 
			//TaskExecutor taskExecutor,
			FraisGestionStep1Processor processor) {

		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);

		return stepBuilderFactory.get("stepFraisGestion_1")
				.<FraisGestionDto, FraisGestionDto>chunk(CachableInfo.activeConfig.getFarisGestionCommitInterval())
				.reader(reader).faultTolerant().skip(FlatFileParseException.class)
				.skipLimit(CachableInfo.activeConfig.getFarisGestionToSkiLimit()).processor(processor).writer(writer)
				.build();
	}

	@Bean
	public Step stepFraisGestion_2(CustomFlatFileItemWriter<TmpAgregFraisGestVueDto> writer, 
			//TaskExecutor taskExecutor,
			FraisGestionStep2Processor<TmpAgregFraisGestVue, TmpAgregFraisGestVueDto> processor) {
		CustomRepositoryItemReader<TmpAgregFraisGestVue> reader = new CustomRepositoryItemReader<>(fluxOutTempVueRep);
		reader.setRepository(fluxOutTempVueRep);
		reader.setMethodName("findAll");
		/**
		 * page size is considered like the number of items that will be proceeded by
		 * transaction and its all treatments. if page size == Integer.MAX_VALUE that
		 * means that there is not paging, and we have only one page which contains all
		 * items.
		 */
		reader.setPageSize(Integer.MAX_VALUE);
		Map<String, Direction> sort = new HashMap<>();
		reader.setSort(sort);
		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
		return stepBuilderFactory.get("stepFraisGestion_2")
				.<TmpAgregFraisGestVue, TmpAgregFraisGestVueDto>chunk(
						CachableInfo.activeConfig.getFarisGestionCommitInterval())
				.reader(reader).faultTolerant().skip(FlatFileParseException.class)
				.skipLimit(CachableInfo.activeConfig.getFarisGestionToSkiLimit()).processor(processor).writer(writer)
				//.taskExecutor(taskExecutor)
				.stream(customFraisGestionVueflatFileItemWriter).build();
	}

	@Bean(name = "fraisGestionJob")
	public Job fraisGestionJob(JobRepository jobRepository, Step stepFraisGestion_1, Step stepFraisGestion_2,
			ProcessFluxListener processListener) {
		JobBuilderFactory jobBuilderFactory = new JobBuilderFactory(jobRepository);
		return jobBuilderFactory.get("fraisGestionJob").repository(jobRepository).incrementer(new RunIdIncrementer())
				.listener(processListener).flow(stepFraisGestion_1).next(stepFraisGestion_2).end().build();
	}

}