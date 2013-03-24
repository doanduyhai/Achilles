package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
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
	private String fqcn;
	private PropertyMeta<Void, ID> idMeta;
	private CounterDao counterDao;
	private PropertyMeta<K, Long> propertyMeta;

	private AchillesInterceptor interceptor;
	protected CompositeHelper compositeHelper;
	protected KeyValueFactory keyValueFactory;
	protected IteratorFactory iteratorFactory;
	protected CompositeKeyFactory compositeKeyFactory;
	protected DynamicCompositeKeyFactory dynamicCompositeKeyFactory;
	protected PersistenceContext<ID> context;

	public CounterWideMapWrapperBuilder(ID id, CounterDao counterDao,
			PropertyMeta<K, Long> propertyMeta)
	{
		this.id = id;
		this.counterDao = counterDao;
		this.propertyMeta = propertyMeta;
	}

	public static <ID, K> CounterWideMapWrapperBuilder<ID, K> builder(ID id, CounterDao counterDao,
			PropertyMeta<K, Long> propertyMeta)
	{
		return new CounterWideMapWrapperBuilder<ID, K>(id, counterDao, propertyMeta);
	}

	public CounterWideMapWrapperBuilder<ID, K> fqcn(String fqcn)
	{
		this.fqcn = fqcn;
		return this;
	}

	public CounterWideMapWrapperBuilder<ID, K> idMeta(PropertyMeta<Void, ID> idMeta)
	{
		this.idMeta = idMeta;
		return this;
	}

	public CounterWideMapWrapperBuilder<ID, K> interceptor(AchillesInterceptor interceptor)
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

	public CounterWideMapWrapperBuilder<ID, K> compositeKeyFactory(
			CompositeKeyFactory compositeKeyFactory)
	{
		this.compositeKeyFactory = compositeKeyFactory;
		return this;
	}

	public CounterWideMapWrapperBuilder<ID, K> dynamicCompositeKeyFactory(
			DynamicCompositeKeyFactory dynamicCompositeKeyFactory)
	{
		this.dynamicCompositeKeyFactory = dynamicCompositeKeyFactory;
		return this;
	}

	public CounterWideMapWrapper<ID, K> build()
	{
		CounterWideMapWrapper<ID, K> wrapper = new CounterWideMapWrapper<ID, K>();
		wrapper.setId(id);
		wrapper.setFqcn(fqcn);
		wrapper.setIdMeta(idMeta);
		wrapper.setCounterDao(counterDao);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setCompositeHelper(compositeHelper);
		wrapper.setIteratorFactory(iteratorFactory);
		wrapper.setCompositeKeyFactory(compositeKeyFactory);
		wrapper.setDynamicCompositeKeyFactory(dynamicCompositeKeyFactory);
		wrapper.setKeyValueFactory(keyValueFactory);
		return wrapper;
	}

}
