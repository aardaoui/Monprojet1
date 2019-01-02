package com.bpc.ayh.batch;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.util.CollectionUtils;

import com.bnppa.coref.common.exception.TechnicalException;
import com.bnppa.referentiels.reftitrerecent.dataobjects.external.TitrePlaceRecentBDT;
import com.bnppa.referentiels.services.locator.ReferentielsServices;
import com.bpc.ayh.batch.readers.CustomPropertyPlaceholderConfigurer;
import com.bpc.ayh.batch.util.BatchConstants;
import com.bpc.ayh.batch.util.CommonUtils;
import com.bpc.ayh.batch.util.Config;
import com.bpc.ayh.batch.util.ConfigurationUtils;
import com.bpc.ayh.common.dto.BatchTraitementDto;
import com.bpc.ayh.common.dto.ReferentielCodesDto;
import com.bpc.ayh.common.util.CachableInfo;
import com.bpc.ayh.service.BatchTraitementService;
import com.bpc.ayh.service.ReferentielCodesService;
import com.bpc.ayh.service.TitreRecentService;

@SpringBootApplication
@Configuration
@ComponentScan(basePackages = { "com.bpc.ayh.service","com.bpc.ayh.batch",
		"com.bpc.ayh.common","com.bpc.ayh.cmn","com.bpc.cro.ws.interceptor" })
@EnableConfigurationProperties
@PropertySources({ 
@PropertySource("classpath:application.properties"),
@PropertySource("classpath:Referentiels.properties") })
@ImportResource({"classpath:service/service.xml"})
@EnableJpaRepositories(basePackages = "com.bpc.ayh.cmn.rep")
public class Application {
	
	/**
	 * The Logger setup
	 */
	static Logger logger = null;

	static {
		String path = Config.getLogFilePath();
		DOMConfigurator.configure(path);
		logger = Logger.getLogger(Application.class);		
	}
	
	static JobParameters jobParameters;
	static String processingType = null;
    
	
	public static void main(String[] args) throws Exception {
		String jobName = null;
		String fileName = null;

		if (args.length == 2 ) {
			jobName = args[0];
			fileName = args[1];
			processingType = BatchConstants.INTEGRATION_FLUX;
			 jobParameters = new JobParametersBuilder().addString("fileName", fileName).toJobParameters();
			
		} else {
			 processingType = args[0];
				
		}     	
    	ConfigurableApplicationContext cfgContext = SpringApplication.run(Application.class, args);        
		if (args.length < 2 && args.length !=1) {
			logger.fatal(ConfigurationUtils.getValue(com.bpc.ayh.batch.util.MsgConstants.EXCEPTION_BAD_PARAM_NUMBER));
			try {
				throw new TechnicalException(ConfigurationUtils.getValue(com.bpc.ayh.batch.util.MsgConstants.EXCEPTION_BAD_PARAM_NUMBER));
			} catch (TechnicalException e) {
				e.printStackTrace();
			}
		}

		if (processingType.equals(BatchConstants.INTEGRATION_FLUX)) {				
			final JobLauncher jobLauncher = (JobLauncher) cfgContext.getBean("jobLauncher");
			final SimpleJobOperator  jobOperator = (SimpleJobOperator) cfgContext.getBean("jobOperator");
			final JobExplorer jobExplorer = (JobExplorer) cfgContext.getBean("jobExplorer");
			final JobRepository jobRepository = (JobRepository) cfgContext.getBean("jobRepository");
			final Job job = (Job) cfgContext.getBean(jobName); 
	    	CachableInfo.jobName = jobName;
	
			try {
	
					// Création d'une ligne de compte-rendu avant le début du traitement
					final BatchTraitementService batchTraitementService = (BatchTraitementService) cfgContext.getBean("compteRenduServiceImpl");
					final ReferentielCodesService referentielCodesService = (ReferentielCodesService) cfgContext.getBean("referentielCodesService");

					BatchTraitementDto batchTraitementDto = new BatchTraitementDto();
					batchTraitementDto.setTypeTraitement("Initial");
					batchTraitementDto.setNomFichier(fileName);
					batchTraitementDto.setDateDebutTraitement(new Date());
					ReferentielCodesDto  refReferentielCodesDto =  referentielCodesService.fetchByCode(BatchConstants.TRT_ENCOURS);
					batchTraitementDto.setIdStatutTraitement(refReferentielCodesDto.getId());

					/* Appel du service pour créer la ligne de compte-rendu avec retour de l'identifiant */
					batchTraitementDto.setId(batchTraitementService.creerLigneBatchTraitement(batchTraitementDto));
	
					
					/*Initialisation du cabhe des apples vers Refco 
					 * Tous les appels ver srefco seron stocké dans cette map, de cete façon le apples ne seront efectués qu'une seul fois 
					 * pour des paramètres simailaires  */
					CachableInfo.lookUpCroMap =  new HashMap<>();
					
					// Mise en cache du compte-rendu
					CachableInfo.batchTraitment = batchTraitementDto;
					JobExecution jobExecution = null;
					 
					
					Set<Long> listIdjobExecution = jobOperator.getRunningExecutions(jobName);
					if (!CollectionUtils.isEmpty(listIdjobExecution)) {
						
						
						
						Long executionId = listIdjobExecution.iterator().next();
		                  jobExecution = jobExplorer.getJobExecution(executionId);

		                  Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
		                  for (StepExecution stepExecution : stepExecutions) {
		                      BatchStatus status = stepExecution.getStatus();
		                      if (status.isRunning() || status == BatchStatus.STOPPING) {
		                          stepExecution.setStatus(BatchStatus.STOPPED);
		                          stepExecution.setEndTime(new Date());
		                          jobRepository.update(stepExecution);
		                      }
		                  }

		                  jobExecution.setStatus(BatchStatus.STOPPED);
		                  jobExecution.setEndTime(new Date());
		                  jobRepository.update(jobExecution);

		                  jobOperator.restart(executionId);
		                  jobExecution = jobRepository.getLastJobExecution(jobName, jobParameters);
					}
					else {
						jobExecution = jobLauncher.run(job, jobParameters);	
					} 		
					
			    	Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();	    	
			    	if (stepExecutions.stream() != null) {
				    	final StepExecution stepExecution = stepExecutions.stream().findFirst().orElse(null);
				    	if (stepExecution != null) {
					    	List<Throwable> exceptions = stepExecution.getFailureExceptions();
					    	if (!exceptions.isEmpty()) {
					    		for (Throwable exception : exceptions) {
					    		   logger.error(exception.getCause());
					    		} 
					    	}
							CachableInfo.typeFlux = Application.determinerTypeFlux(jobName);
							batchTraitementDto.setIdTypeFlux(CachableInfo.mapTypesFlux.get(CachableInfo.typeFlux).getId());
					    	batchTraitementDto.setIdJobInstance(jobExecution.getJobInstance().getId());
						}
			    	}
			    	batchTraitementDto.setDateFinTraitement(new Date());
			    	batchTraitementService.majLigneBatchTraitement(batchTraitementDto);
			    //	compteRenduService.envoyerMail(jobExecution.getJobInstance().getId());
	
					System.out.println("Exit Status : " + jobExecution.getStatus());
					System.out.println("Excution Duration :  Begin  ".concat(jobExecution.getStartTime().toString()).concat(" End  ".concat(jobExecution.getEndTime().toString())));
					
				cfgContext.close();				
			  }  catch (Throwable th) {
					logger.error(th.getMessage(), th);
			  }
		} else {
			if (processingType.equals(BatchConstants.CHARGEMENT_TITRE)) {
				ReferentielCodesService referentielCodesService = (ReferentielCodesService) cfgContext.getBean("referentielCodesService");
				CommonUtils.chargementReferentielCodes(referentielCodesService);			
				
				
				ReferentielsServices referentielsServices = (ReferentielsServices)cfgContext.getBean("referentielsServices");
				Collection<TitrePlaceRecentBDT> titres = referentielsServices.getTitreRecentServices().getAllTitresPlaces();
				TitreRecentService titreRecentService = (TitreRecentService) cfgContext.getBean("titreRecentServiceImpl");
				titreRecentService.refreshTitre(titres);
			} else {
				throw new TechnicalException(ConfigurationUtils.getValue(com.bpc.ayh.batch.util.MsgConstants.EXCEPTION_BAD_PARAMETER));
			}
		}
	}
    
    
    @Bean
    public static CustomPropertyPlaceholderConfigurer propertyConfigurer() {
    	CustomPropertyPlaceholderConfigurer propertyConfigurer = new CustomPropertyPlaceholderConfigurer();
    	propertyConfigurer.setSystemPropertiesModeName("SYSTEM_PROPERTIES_MODE_OVERRIDE");
    	propertyConfigurer.setLocations(new ClassPathResource("application.properties"), new ClassPathResource("Referentiels.properties"));
    	propertyConfigurer.setIgnoreUnresolvablePlaceholders(true);
    	return propertyConfigurer;
    }

	private static String determinerTypeFlux(final String jobName) {
		if (jobName.toUpperCase().contains(BatchConstants.PREV)) {
			return BatchConstants.PREV;
		}
		if (jobName.toUpperCase().contains(BatchConstants.REAL)) {
			return BatchConstants.REAL;
		}
		if (jobName.toUpperCase().contains(BatchConstants.FRAISG)) {
			return BatchConstants.FRAISG;
		}
		if (jobName.toUpperCase().contains(BatchConstants.REAL)) {
			return BatchConstants.CHARGEMENT_TITRE;
		}		
		return "";
	}


}