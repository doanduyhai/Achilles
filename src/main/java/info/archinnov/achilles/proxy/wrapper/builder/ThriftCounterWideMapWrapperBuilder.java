package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.ThriftCompositeHelper;
import info.archinnov.achilles.iterator.factory.ThriftIteratorFactory;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;
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

	private AchillesEntityInterceptor<?> interceptor;
	protected ThriftCompositeHelper thriftCompositeHelper;
	protected ThriftKeyValueFactory thriftKeyValueFactory;
	protected ThriftIteratorFactory thriftIteratorFactory;
	protected ThriftCompositeFactory thriftCompositeFactory;
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

	public ThriftCounterWideMapWrapperBuilder<K> interceptor(
			AchillesEntityInterceptor<?> interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public ThriftCounterWideMapWrapperBuilder<K> context(ThriftPersistenceContext context)
	{
		this.context = context;
		return this;
	}

	public ThriftCounterWideMapWrapperBuilder<K> thriftCompositeHelper(
			ThriftCompositeHelper thriftCompositeHelper)
	{
		this.thriftCompositeHelper = thriftCompositeHelper;
		return this;
	}

	public ThriftCounterWideMapWrapperBuilder<K> thriftKeyValueFactory(
			ThriftKeyValueFactory thriftKeyValueFactory)
	{
		this.thriftKeyValueFactory = thriftKeyValueFactory;
		return this;
	}

	public ThriftCounterWideMapWrapperBuilder<K> thriftIteratorFactory(
			ThriftIteratorFactory thriftIteratorFactory)
	{
		this.thriftIteratorFactory = thriftIteratorFactory;
		return this;
	}

	public ThriftCounterWideMapWrapperBuilder<K> thriftCompositeFactory(
			ThriftCompositeFactory thriftCompositeFactory)
	{
		this.thriftCompositeFactory = thriftCompositeFactory;
		return this;
	}

	public ThriftCounterWideMapWrapper<K> build()
	{
		ThriftCounterWideMapWrapper<K> wrapper = new ThriftCounterWideMapWrapper<K>();
		wrapper.setId(id);
		wrapper.setWideMapCounterDao(wideMapCounterDao);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setCompositeHelper(thriftCompositeHelper);
		wrapper.setIteratorFactory(thriftIteratorFactory);
		wrapper.setCompositeKeyFactory(thriftCompositeFactory);
		wrapper.setKeyValueFactory(thriftKeyValueFactory);
		wrapper.setContext(context);
		return wrapper;
	}

}
