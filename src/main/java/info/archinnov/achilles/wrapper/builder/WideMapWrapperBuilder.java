package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;
import info.archinnov.achilles.wrapper.WideMapWrapper;

/**
 * WideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapperBuilder<ID, K, V>
{
	private ID id;
	private GenericDynamicCompositeDao<ID> dao;
	private PropertyMeta<K, V> wideMapMeta;
	private AchillesInterceptor interceptor;

	public WideMapWrapperBuilder(ID id, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.id = id;
		this.dao = dao;
		this.wideMapMeta = wideMapMeta;
	}

	public static <ID, K, V> WideMapWrapperBuilder<ID, K, V> builder(ID id,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<K, V> wideMapMeta)
	{
		return new WideMapWrapperBuilder<ID, K, V>(id, dao, wideMapMeta);
	}

	public WideMapWrapperBuilder<ID, K, V> interceptor(AchillesInterceptor interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public WideMapWrapper<ID, K, V> build()
	{
		WideMapWrapper<ID, K, V> wrapper = new WideMapWrapper<ID, K, V>();
		build(wrapper);
		return wrapper;
	}

	protected void build(WideMapWrapper<ID, K, V> wrapper)
	{
		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setWideMapMeta(wideMapMeta);
		wrapper.setInterceptor(interceptor);
	}

}
