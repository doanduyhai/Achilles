package info.archinnov.achilles.proxy.interceptor;

import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.wrapper.builder.CounterWideMapWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.CounterWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.JoinWideMapWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.WideMapWrapperBuilder;
import me.prettyprint.hector.api.beans.Composite;

/**
 * JpaEntityInterceptor
 * 
 * @author DuyHai DOAN
 * 
 */
public class JpaEntityInterceptor<T> extends AchillesJpaEntityInterceptor<T>
{

	private CompositeHelper compositeHelper = new CompositeHelper();
	private KeyValueFactory keyValueFactory = new KeyValueFactory();
	private IteratorFactory iteratorFactory = new IteratorFactory();
	private CompositeFactory compositeFactory = new CompositeFactory();

	public JpaEntityInterceptor() {
		super.loader = new ThriftEntityLoader();
		super.persister = new ThriftEntityPersister();
		super.proxifier = new ThriftEntityProxifier();
	}

	@Override
	protected Object buildCounterWrapper(PropertyMeta<?, ?> propertyMeta)
	{
		Object result;
		ThriftPersistenceContext thriftContext = (ThriftPersistenceContext) context;
		CounterProperties counterProperties = propertyMeta.getCounterProperties();
		Composite keyComp = compositeFactory.createKeyForCounter(counterProperties.getFqcn(), key,
				counterProperties.getIdMeta());
		Composite comp = compositeFactory.createBaseForCounterGet(propertyMeta);
		result = CounterWrapperBuilder.builder(thriftContext) //
				.counterDao(thriftContext.getCounterDao())
				//
				.columnName(comp)
				//
				.readLevel(propertyMeta.getReadConsistencyLevel())
				//
				.writeLevel(propertyMeta.getWriteConsistencyLevel())
				//
				.key(keyComp)
				//
				.build();
		return result;
	}

	@Override
	protected <K, V> Object buildWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		ThriftPersistenceContext thriftContext = (ThriftPersistenceContext) context;
		String columnFamilyName = context.isWideRow() ? context
				.getEntityMeta()
				.getColumnFamilyName() : propertyMeta.getExternalCFName();

		ThriftGenericWideRowDao wideRowDao = thriftContext.findWideRowDao(columnFamilyName);

		return WideMapWrapperBuilder //
				.builder(key, wideRowDao, propertyMeta)
				//
				.context(thriftContext)
				//
				.interceptor(this)
				//
				.compositeHelper(compositeHelper)
				//
				.keyValueFactory(keyValueFactory)
				//
				.iteratorFactory(iteratorFactory)
				//
				.compositeFactory(compositeFactory)
				//
				.build();
	}

	@Override
	protected <K> Object buildCounterWideMapWrapper(PropertyMeta<K, Counter> propertyMeta)
	{
		ThriftPersistenceContext thriftContext = (ThriftPersistenceContext) context;
		ThriftGenericWideRowDao counterWideMapDao = thriftContext.findWideRowDao(propertyMeta
				.getExternalCFName());

		return CounterWideMapWrapperBuilder //
				.builder(key, counterWideMapDao, propertyMeta)
				//
				.interceptor(this)
				//
				.context(thriftContext)
				//
				.compositeHelper(compositeHelper)
				//
				.keyValueFactory(keyValueFactory)
				//
				.iteratorFactory(iteratorFactory)
				//
				.compositeFactory(compositeFactory)
				//
				.build();
	}

	@Override
	protected <K, V> Object buildJoinWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{

		ThriftPersistenceContext thriftContext = (ThriftPersistenceContext) context;
		String columnFamilyName = context.isWideRow() ? context
				.getEntityMeta()
				.getColumnFamilyName() : propertyMeta.getExternalCFName();
		ThriftGenericWideRowDao wideRowDao = thriftContext.findWideRowDao(columnFamilyName);

		return JoinWideMapWrapperBuilder //
				.builder(key, wideRowDao, propertyMeta)
				//
				.interceptor(this)
				//
				.context(thriftContext)
				//
				.compositeHelper(compositeHelper)
				//
				.compositeFactory(compositeFactory)
				//
				.proxifier(proxifier)
				//
				.iteratorFactory(iteratorFactory)
				//
				.keyValueFactory(keyValueFactory)
				//
				.loader((ThriftEntityLoader) loader)
				//
				.persister((ThriftEntityPersister) persister)
				//
				.build();
	}

	@Override
	protected <K, V> Object buildWideRowWrapper(PropertyMeta<K, V> propertyMeta)
	{
		ThriftPersistenceContext thriftContext = (ThriftPersistenceContext) context;
		ThriftGenericWideRowDao wideRowDao = thriftContext.findWideRowDao(context
				.getEntityMeta()
				.getColumnFamilyName());

		return WideMapWrapperBuilder.builder(key, wideRowDao, propertyMeta) //
				.interceptor(this)
				//
				.context(thriftContext)
				//
				.compositeHelper(compositeHelper)
				//
				.keyValueFactory(keyValueFactory)
				//
				.iteratorFactory(iteratorFactory)
				//
				.compositeFactory(compositeFactory)
				//
				.build();
	}

}
