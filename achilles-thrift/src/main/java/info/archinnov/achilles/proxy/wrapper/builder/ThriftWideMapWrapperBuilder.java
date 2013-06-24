package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.proxy.wrapper.ThriftWideMapWrapper;

/**
 * ThriftWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftWideMapWrapperBuilder<K, V>
{
	private Object id;
	private ThriftGenericWideRowDao dao;
	private PropertyMeta<K, V> wideMapMeta;
	private ThriftEntityInterceptor<?> interceptor;
	private ThriftPersistenceContext context;

	public ThriftWideMapWrapperBuilder(Object id, ThriftGenericWideRowDao dao,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.id = id;
		this.dao = dao;
		this.wideMapMeta = wideMapMeta;
	}

	public static <K, V> ThriftWideMapWrapperBuilder<K, V> builder(Object id,
			ThriftGenericWideRowDao dao, PropertyMeta<K, V> wideMapMeta)
	{
		return new ThriftWideMapWrapperBuilder<K, V>(id, dao, wideMapMeta);
	}

	public ThriftWideMapWrapperBuilder<K, V> interceptor(ThriftEntityInterceptor<?> interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public ThriftWideMapWrapperBuilder<K, V> context(ThriftPersistenceContext context)
	{
		this.context = context;
		return this;
	}

	public ThriftWideMapWrapper<K, V> build()
	{
		ThriftWideMapWrapper<K, V> wrapper = new ThriftWideMapWrapper<K, V>();
		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setWideMapMeta(wideMapMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setContext(context);
		return wrapper;
	}

}
