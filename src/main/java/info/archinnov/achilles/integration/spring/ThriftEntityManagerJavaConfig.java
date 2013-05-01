package info.archinnov.achilles.integration.spring;

import info.archinnov.achilles.entity.manager.ThriftEntityManager;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * ThriftEntityManagerJavaConfig
 * 
 * @author DuyHai DOAN
 * 
 */
@Configuration
public class ThriftEntityManagerJavaConfig extends AbstractThriftEntityManagerFactory
{

	@Bean
	public ThriftEntityManager getEntityManager()
	{
		return em;
	}
}
