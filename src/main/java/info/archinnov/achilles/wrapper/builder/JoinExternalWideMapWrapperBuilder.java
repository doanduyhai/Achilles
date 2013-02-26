package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
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
	private GenericCompositeDao<ID, JOIN_ID> dao;
	private PropertyMeta<K, V> joinExternalWideMapMeta;
	private AchillesInterceptor interceptor;
	private EntityPersister persister;
	private EntityLoader loader;
	private EntityHelper entityHelper;
	private CompositeHelper compositeHelper;
	private CompositeKeyFactory compositeKeyFactory;
	private KeyValueFactory keyValueFactory;
	private IteratorFactory iteratorFactory;

	public JoinExternalWideMapWrapperBuilder(ID id, GenericCompositeDao<ID, JOIN_ID> dao,
			PropertyMeta<K, V> joinExternalWideMapMeta)
	{
		this.id = id;
		this.dao = dao;
		this.joinExternalWideMapMeta = joinExternalWideMapMeta;
	}

	public static <ID, JOIN_ID, K, V> JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> builder(
			ID id, GenericCompositeDao<ID, JOIN_ID> dao, PropertyMeta<K, V> joinExternalWideMapMeta)
	{
		return new JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V>(id, dao,
				joinExternalWideMapMeta);
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> interceptor(
			AchillesInterceptor interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public JoinExternalWideMapWrapperBuilder<ID, JOIN_ID, K, V> entityHelper(
			EntityHelper entityHelper)
	{
		this.entityHelper = entityHelper;
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
		wrapper.setExternalWideMapMeta(joinExternalWideMapMeta);
		wrapper.setExternalWideMapDao(dao);
		wrapper.setInterceptor(interceptor);
		wrapper.setEntityHelper(entityHelper);
		wrapper.setCompositeHelper(compositeHelper);
		wrapper.setCompositeKeyFactory(compositeKeyFactory);
		wrapper.setIteratorFactory(iteratorFactory);
		wrapper.setKeyValueFactory(keyValueFactory);
		wrapper.setLoader(loader);
		wrapper.setPersister(persister);
		return wrapper;
	}

}
