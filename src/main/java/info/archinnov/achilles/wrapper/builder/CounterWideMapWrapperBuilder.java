package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;
import info.archinnov.achilles.wrapper.CounterWideMapWrapper;

/**
 * CounterWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterWideMapWrapperBuilder<ID, K>
{
	private ID id;
	private GenericColumnFamilyDao<ID, Long> wideMapCounterDao;
	private PropertyMeta<K, Counter> propertyMeta;

	private AchillesInterceptor<ID> interceptor;
	protected CompositeHelper compositeHelper;
	protected KeyValueFactory keyValueFactory;
	protected IteratorFactory iteratorFactory;
	protected CompositeFactory compositeFactory;
	protected PersistenceContext<ID> context;

	public CounterWideMapWrapperBuilder(ID id, GenericColumnFamilyDao<ID, Long> wideMapCounterDao,
			PropertyMeta<K, Counter> propertyMeta)
	{
		this.id = id;
		this.wideMapCounterDao = wideMapCounterDao;
		this.propertyMeta = propertyMeta;
	}

	public static <ID, K> CounterWideMapWrapperBuilder<ID, K> builder(ID id,
			GenericColumnFamilyDao<ID, Long> wideMapCounterDao,
			PropertyMeta<K, Counter> propertyMeta)
	{
		return new CounterWideMapWrapperBuilder<ID, K>(id, wideMapCounterDao, propertyMeta);
	}

	public CounterWideMapWrapperBuilder<ID, K> interceptor(AchillesInterceptor<ID> interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public CounterWideMapWrapperBuilder<ID, K> context(PersistenceContext<ID> context)
	{
		this.context = context;
		return this;
	}

	public CounterWideMapWrapperBuilder<ID, K> compositeHelper(CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
		return this;
	}

	public CounterWideMapWrapperBuilder<ID, K> keyValueFactory(KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
		return this;
	}

	public CounterWideMapWrapperBuilder<ID, K> iteratorFactory(IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
		return this;
	}

	public CounterWideMapWrapperBuilder<ID, K> compositeFactory(CompositeFactory compositeFactory)
	{
		this.compositeFactory = compositeFactory;
		return this;
	}

	public CounterWideMapWrapper<ID, K> build()
	{
		CounterWideMapWrapper<ID, K> wrapper = new CounterWideMapWrapper<ID, K>();
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
