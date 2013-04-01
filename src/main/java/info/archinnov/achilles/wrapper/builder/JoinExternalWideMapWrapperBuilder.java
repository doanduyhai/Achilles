package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;
import info.archinnov.achilles.wrapper.JoinExternalWideMapWrapper;

/**
 * JoinExternalWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V>
{
	private ID id;
	private GenericColumnFamilyDao<ID, JOIN_ID> dao;
	private PropertyMeta<K, V> joinExternalWideMapMeta;
	private AchillesInterceptor<ID> interceptor;
	private EntityPersister persister;
	private EntityLoader loader;
	private EntityProxifier proxifier;
	private CompositeHelper compositeHelper;
	private CompositeKeyFactory compositeKeyFactory;
	private KeyValueFactory keyValueFactory;
	private IteratorFactory iteratorFactory;
	private PersistenceContext<ID> context;

	public JoinExternalWideMapWrapperBuilder(ID id, GenericColumnFamilyDao<ID, JOIN_ID> dao,
			PropertyMeta<K, V> joinExternalWideMapMeta)
	{
		this.dao = dao;
		this.id = id;
		this.joinExternalWideMapMeta = joinExternalWideMapMeta;
	}

	public static <ID, JOIN_ID, K, V> JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> builder(
			ID id, GenericColumnFamilyDao<ID, JOIN_ID> dao, PropertyMeta<K, V> joinExternalWideMapMeta)
	{
		return new JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V>(id, dao,
				joinExternalWideMapMeta);
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> interceptor(
			AchillesInterceptor<ID> interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> context(
			PersistenceContext<ID> context)
	{
		this.context = context;
		return this;
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> proxifier(EntityProxifier proxifier)
	{
		this.proxifier = proxifier;
		return this;
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> persister(EntityPersister persister)
	{
		this.persister = persister;
		return this;
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> loader(EntityLoader loader)
	{
		this.loader = loader;
		return this;
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> compositeHelper(
			CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
		return this;
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> compositeKeyFactory(
			CompositeKeyFactory compositeKeyFactory)
	{
		this.compositeKeyFactory = compositeKeyFactory;
		return this;
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> keyValueFactory(
			KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
		return this;
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> iteratorFactory(
			IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
		return this;
	}

	public JoinExternalWideMapWrapper<ID, JOIN_ID, K, V> build()
	{
		JoinExternalWideMapWrapper<ID, JOIN_ID, K, V> wrapper = new JoinExternalWideMapWrapper<ID, JOIN_ID, K, V>();

		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setExternalWideMapMeta(joinExternalWideMapMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setEntityProxifier(proxifier);
		wrapper.setCompositeHelper(compositeHelper);
		wrapper.setCompositeKeyFactory(compositeKeyFactory);
		wrapper.setIteratorFactory(iteratorFactory);
		wrapper.setKeyValueFactory(keyValueFactory);
		wrapper.setLoader(loader);
		wrapper.setPersister(persister);
		wrapper.setContext(context);
		return wrapper;
	}

}
