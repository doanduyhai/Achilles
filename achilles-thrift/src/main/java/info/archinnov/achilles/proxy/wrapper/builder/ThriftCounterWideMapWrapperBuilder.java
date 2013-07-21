package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.proxy.wrapper.ThriftCounterWideMapWrapper;
import info.archinnov.achilles.type.Counter;

/**
 * ThriftCounterWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCounterWideMapWrapperBuilder<K>
{
	private Object id;
	private ThriftGenericWideRowDao wideMapCounterDao;
	private PropertyMeta<K, Counter> propertyMeta;

	private ThriftEntityInterceptor<?> interceptor;
	protected ThriftPersistenceContext context;

	public ThriftCounterWideMapWrapperBuilder(Object id, ThriftGenericWideRowDao wideMapCounterDao,
			PropertyMeta<K, Counter> propertyMeta)
	{
		this.id = id;
		this.wideMapCounterDao = wideMapCounterDao;
		this.propertyMeta = propertyMeta;
	}

	public static <K> ThriftCounterWideMapWrapperBuilder<K> builder(Object id,
			ThriftGenericWideRowDao wideMapCounterDao, PropertyMeta<K, Counter> propertyMeta)
	{
		return new ThriftCounterWideMapWrapperBuilder<K>(id, wideMapCounterDao, propertyMeta);
	}

	public ThriftCounterWideMapWrapperBuilder<K> interceptor(ThriftEntityInterceptor<?> interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public ThriftCounterWideMapWrapperBuilder<K> context(ThriftPersistenceContext context)
	{
		this.context = context;
		return this;
	}

	public ThriftCounterWideMapWrapper<K> build()
	{
		ThriftCounterWideMapWrapper<K> wrapper = new ThriftCounterWideMapWrapper<K>();
		wrapper.setId(id);
		wrapper.setWideMapCounterDao(wideMapCounterDao);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setContext(context);
		return wrapper;
	}

}
