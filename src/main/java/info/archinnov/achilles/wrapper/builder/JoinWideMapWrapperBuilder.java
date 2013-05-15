package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.AchillesEntityProxifier;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesJpaEntityInterceptor;
import info.archinnov.achilles.wrapper.JoinWideMapWrapper;

/**
 * JoinExternalWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V>
{
	private ID id;
	private ThriftGenericWideRowDao<ID, JOIN_ID> dao;
	private PropertyMeta<K, V> joinExternalWideMapMeta;
	private AchillesJpaEntityInterceptor<ID, ?> interceptor;
	private ThriftEntityPersister persister;
	private ThriftEntityLoader loader;
	private AchillesEntityProxifier proxifier;
	private CompositeHelper compositeHelper;
	private CompositeFactory compositeFactory;
	private KeyValueFactory keyValueFactory;
	private IteratorFactory iteratorFactory;
	private ThriftPersistenceContext<ID> context;

	public JoinWideMapWrapperBuilder(ID id, ThriftGenericWideRowDao<ID, JOIN_ID> dao,
			PropertyMeta<K, V> joinExternalWideMapMeta)
	{
		this.dao = dao;
		this.id = id;
		this.joinExternalWideMapMeta = joinExternalWideMapMeta;
	}

	public static <ID, JOIN_ID, K, V> JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> builder(ID id,
			ThriftGenericWideRowDao<ID, JOIN_ID> dao, PropertyMeta<K, V> joinExternalWideMapMeta)
	{
		return new JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V>(id, dao, joinExternalWideMapMeta);
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> interceptor(
			AchillesJpaEntityInterceptor<ID, ?> interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> context(ThriftPersistenceContext<ID> context)
	{
		this.context = context;
		return this;
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> proxifier(AchillesEntityProxifier proxifier)
	{
		this.proxifier = proxifier;
		return this;
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> persister(ThriftEntityPersister persister)
	{
		this.persister = persister;
		return this;
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> loader(ThriftEntityLoader loader)
	{
		this.loader = loader;
		return this;
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> compositeHelper(
			CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
		return this;
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> compositeFactory(
			CompositeFactory compositeFactory)
	{
		this.compositeFactory = compositeFactory;
		return this;
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> keyValueFactory(
			KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
		return this;
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> iteratorFactory(
			IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
		return this;
	}

	public JoinWideMapWrapper<ID, JOIN_ID, K, V> build()
	{
		JoinWideMapWrapper<ID, JOIN_ID, K, V> wrapper = new JoinWideMapWrapper<ID, JOIN_ID, K, V>();

		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setExternalWideMapMeta(joinExternalWideMapMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setEntityProxifier(proxifier);
		wrapper.setCompositeHelper(compositeHelper);
		wrapper.setCompositeKeyFactory(compositeFactory);
		wrapper.setIteratorFactory(iteratorFactory);
		wrapper.setKeyValueFactory(keyValueFactory);
		wrapper.setLoader(loader);
		wrapper.setPersister(persister);
		wrapper.setContext(context);
		return wrapper;
	}

}
