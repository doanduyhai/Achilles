package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.proxy.wrapper.ThriftJoinWideMapWrapper;

/**
 * ThriftJoinWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftJoinWideMapWrapperBuilder<K, V>
{
	private Object id;
	private ThriftGenericWideRowDao dao;
	private PropertyMeta<K, V> joinExternalWideMapMeta;
	private ThriftEntityInterceptor<?> interceptor;
	private ThriftPersistenceContext context;

	public ThriftJoinWideMapWrapperBuilder(Object id, ThriftGenericWideRowDao dao,
			PropertyMeta<K, V> joinExternalWideMapMeta)
	{
		this.dao = dao;
		this.id = id;
		this.joinExternalWideMapMeta = joinExternalWideMapMeta;
	}

	public static <K, V> ThriftJoinWideMapWrapperBuilder<K, V> builder(Object id,
			ThriftGenericWideRowDao dao, PropertyMeta<K, V> joinExternalWideMapMeta)
	{
		return new ThriftJoinWideMapWrapperBuilder<K, V>(id, dao, joinExternalWideMapMeta);
	}

	public ThriftJoinWideMapWrapperBuilder<K, V> interceptor(ThriftEntityInterceptor<?> interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public ThriftJoinWideMapWrapperBuilder<K, V> context(ThriftPersistenceContext context)
	{
		this.context = context;
		return this;
	}

	public ThriftJoinWideMapWrapper<K, V> build()
	{
		ThriftJoinWideMapWrapper<K, V> wrapper = new ThriftJoinWideMapWrapper<K, V>();

		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setExternalWideMapMeta(joinExternalWideMapMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setContext(context);
		return wrapper;
	}

}
