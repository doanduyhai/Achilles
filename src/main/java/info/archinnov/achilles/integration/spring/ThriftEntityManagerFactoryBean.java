package info.archinnov.achilles.integration.spring;

import info.archinnov.achilles.entity.manager.ThriftEntityManager;

import org.springframework.beans.factory.FactoryBean;

/**
 * ThriftEntityManagerFactoryBean
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityManagerFactoryBean extends AbstractThriftEntityManagerFactory implements
		FactoryBean<ThriftEntityManager>
{

	public ThriftEntityManager getObject() throws Exception
	{
		return em;
	}

	@Override
	public Class<?> getObjectType()
	{
		return ThriftEntityManager.class;
	}

	@Override
	public boolean isSingleton()
	{
		return true;
	}

}
