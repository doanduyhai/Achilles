package info.archinnov.achilles.proxy;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.type.Counter;

/**
 * ThriftEntityInterceptor
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityInterceptor<T> extends AchillesEntityInterceptor<CQLPersistenceContext, T>
{

	public CQLEntityInterceptor() {
		super.loader = new CQLEntityLoader();
		super.persister = new CQLEntityPersister();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.proxy.AchillesEntityInterceptor#buildCounterWrapper(info.archinnov.achilles.entity.metadata.PropertyMeta)
	 */
	@Override
	protected Object buildCounterWrapper(PropertyMeta<?, ?> propertyMeta)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.proxy.AchillesEntityInterceptor#buildJoinWideMapWrapper(info.archinnov.achilles.entity.metadata.PropertyMeta)
	 */
	@Override
	protected <K, V> Object buildJoinWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.proxy.AchillesEntityInterceptor#buildCounterWideMapWrapper(info.archinnov.achilles.entity.metadata.PropertyMeta)
	 */
	@Override
	protected <K> Object buildCounterWideMapWrapper(PropertyMeta<K, Counter> propertyMeta)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.proxy.AchillesEntityInterceptor#buildWideMapWrapper(info.archinnov.achilles.entity.metadata.PropertyMeta)
	 */
	@Override
	protected <K, V> Object buildWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see info.archinnov.achilles.proxy.AchillesEntityInterceptor#buildWideRowWrapper(info.archinnov.achilles.entity.metadata.PropertyMeta)
	 */
	@Override
	protected <K, V> Object buildWideRowWrapper(PropertyMeta<K, V> propertyMeta)
	{
		// TODO Auto-generated method stub
		return null;
	}

}
