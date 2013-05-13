package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesJpaEntityInterceptor;
import info.archinnov.achilles.wrapper.WideMapWrapper;

/**
 * ExternalWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapperBuilder<ID, K, V>
{
	private ID id;
	private ThriftGenericWideRowDao<ID, V> dao;
	private PropertyMeta<K, V> wideMapMeta;
	private AchillesJpaEntityInterceptor<ID, ?> interceptor;
	private CompositeHelper compositeHelper;
	private KeyValueFactory keyValueFactory;
	private IteratorFactory iteratorFactory;
	private CompositeFactory compositeFactory;
	private ThriftPersistenceContext<ID> context;

	public WideMapWrapperBuilder(ID id, ThriftGenericWideRowDao<ID, V> dao,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.id = id;
		this.dao = dao;
		this.wideMapMeta = wideMapMeta;
	}

	public static <ID, K, V> WideMapWrapperBuilder<ID, K, V> builder(ID id,
			ThriftGenericWideRowDao<ID, V> dao, PropertyMeta<K, V> wideMapMeta)
	{
		return new WideMapWrapperBuilder<ID, K, V>(id, dao, wideMapMeta);
	}

	public WideMapWrapperBuilder<ID, K, V> interceptor(
			AchillesJpaEntityInterceptor<ID, ?> interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> context(ThriftPersistenceContext<ID> context)
	{
		this.context = context;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> compositeHelper(CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> keyValueFactory(KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> iteratorFactory(IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> compositeFactory(CompositeFactory compositeFactory)
	{
		this.compositeFactory = compositeFactory;
		return this;
	}

	public WideMapWrapper<ID, K, V> build()
	{
		WideMapWrapper<ID, K, V> wrapper = new WideMapWrapper<ID, K, V>();
		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setWideMapMeta(wideMapMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setCompositeHelper(compositeHelper);
		wrapper.setCompositeKeyFactory(compositeFactory);
		wrapper.setIteratorFactory(iteratorFactory);
		wrapper.setKeyValueFactory(keyValueFactory);
		wrapper.setContext(context);
		return wrapper;
	}

}
