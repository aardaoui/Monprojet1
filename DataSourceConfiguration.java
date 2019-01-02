package com.bpc.ayh.batch;

import java.sql.SQLException;
import java.util.Properties;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@PropertySource("classpath:application.properties")
public class DataSourceConfiguration {
	
	private static final String ENTITYMANAGER_PACKAGES_TO_SCAN = "com.bpc.ayh.cmn.rules.entity";
	private static final String ENTITYMANAGER_PACKAGES_TO_SCAN_BATCH = "com.bpc.ayh.cmn.entity";
	
	private static final String HIBERNATE_DIALECT = "org.hibernate.dialect.OracleDialect";
	private static final String HIBERNATE_SHOW_SQL = "false";
	private static final String HIBERNATE_CURRENT_SESSION_CONTEXT_CLASS = "org.hibernate.context.internal.ThreadLocalSessionContext";
	
	@Value("${batch.jdbc.driver}")
    private String driver;

	@Value("${batch.jdbc.user}")
    private String username;

	@Value("${batch.jdbc.password}")
    private String password;

	@Value("${batch.jdbc.url}")
    private String url;

    public DataSourceConfiguration() {
    	
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Bean
    public DataSource dataSource() throws SQLException {
    	DriverManagerDataSource dataSource = new DriverManagerDataSource();
    	dataSource.setDriverClassName(driver);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setUrl(url);
        return dataSource;
    }
    
	Properties additionalJpaProperties(){
        Properties properties = new Properties();
        properties.setProperty("hibernate.dialect","org.hibernate.dialect.Oracle10gDialect");
        properties.setProperty("hibernate.show_sql","true");
        properties.setProperty("spring.jpa.properties.hibernate.current_session_context_class","org.springframework.orm.hibernate5.SpringSessionContext");
        properties.setProperty("hibernate.id.new_generator_mappings","false");
        return properties;
    }
	
    @Bean(name="entityManagerFactory")
    public LocalContainerEntityManagerFactoryBean entityManagerFactory() throws SQLException {
          LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
          em.setDataSource(dataSource());
          em.setJpaProperties(additionalJpaProperties());
          String[] packagesToScan = new String[] { "com.bpc.ayh.common","com.bpc.ayh.cmn.rep",ENTITYMANAGER_PACKAGES_TO_SCAN,ENTITYMANAGER_PACKAGES_TO_SCAN_BATCH};
          em.setPackagesToScan(packagesToScan);     
          JpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();          
          em.setJpaVendorAdapter(vendorAdapter);
          return em;
    }
    
    @Bean(name = "transactionManager")
    public PlatformTransactionManager  getTransactionManager(EntityManagerFactory entityManagerFactory) {
    	JpaTransactionManager transactionManager = new JpaTransactionManager(entityManagerFactory);
        return transactionManager;
    }
 
    
    
   
}
