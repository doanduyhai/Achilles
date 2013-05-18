package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesJpaEntityInterceptor;
import info.archinnov.achilles.wrapper.CounterWideMapWrapper;

/**
 * CounterWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterWideMapWrapperBuilder<K>
{
	private Object id;
	private ThriftGenericWideRowDao wideMapCounterDao;
	private PropertyMeta<K, Counter> propertyMeta;

	private AchillesJpaEntityInterceptor<?> interceptor;
	protected CompositeHelper compositeHelper;
	protected KeyValueFactory keyValueFactory;
	protected IteratorFactory iteratorFactory;
	protected CompositeFactory compositeFactory;
	protected ThriftPersistenceContext context;

	public CounterWideMapWrapperBuilder(Object id, ThriftGenericWideRowDao wideMapCounterDao,
			PropertyMeta<K, Counter> propertyMeta)
	{
		this.id = id;
		this.wideMapCounterDao = wideMapCounterDao;
		this.propertyMeta = propertyMeta;
	}

	public static <K> CounterWideMapWrapperBuilder<K> builder(Object id,
			ThriftGenericWideRowDao wideMapCounterDao, PropertyMeta<K, Counter> propertyMeta)
	{
		return new CounterWideMapWrapperBuilder<K>(id, wideMapCounterDao, propertyMeta);
	}

	public CounterWideMapWrapperBuilder<K> interceptor(AchillesJpaEntityInterceptor<?> interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public CounterWideMapWrapperBuilder<K> context(ThriftPersistenceContext context)
	{
		this.context = context;
		return this;
	}

	public CounterWideMapWrapperBuilder<K> compositeHelper(CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
		return this;
	}

	public CounterWideMapWrapperBuilder<K> keyValueFactory(KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
		return this;
	}

	public CounterWideMapWrapperBuilder<K> iteratorFactory(IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
		return this;
	}

	public CounterWideMapWrapperBuilder<K> compositeFactory(CompositeFactory compositeFactory)
	{
		this.compositeFactory = compositeFactory;
		return this;
	}

	public CounterWideMapWrapper<K> build()
	{
		CounterWideMapWrapper<K> wrapper = new CounterWideMapWrapper<K>();
		wrapper.setId(id);
		wrapper.setWideMapCounterDao(wideMapCounterDao);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setCompositeHelper(compositeHelper);
		wrapper.setIteratorFactory(iteratorFactory);
		wrapper.setCompositeKeyFactory(compositeFactory);
		wrapper.setKeyValueFactory(keyValueFactory);
		wrapper.setContext(context);
		return wrapper;
	}

}
