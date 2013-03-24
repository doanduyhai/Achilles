package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.wrapper.JoinWideMapWrapper;

/**
 * JoinWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> extends WideMapWrapperBuilder<ID, K, V>
{

	private EntityPersister persister;
	private EntityLoader loader;

	public JoinWideMapWrapperBuilder(PersistenceContext<ID> context, ID id,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<K, V> joinWideMapMeta)
	{
		super(id, dao, joinWideMapMeta);
		super.context = context;
	}

	public static <ID, JOIN_ID, K, V> JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> builder(
			PersistenceContext<ID> context, ID id, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<K, V> wideMapMeta)
	{
		return new JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V>(context, id, dao, wideMapMeta);
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> persister(EntityPersister persister)
	{
		this.persister = persister;
		return this;
	}

	public JoinWideMapWrapperBuilder<ID, JOIN_ID, K, V> loader(EntityLoader loader)
	{
		this.loader = loader;
		return this;
	}

	public JoinWideMapWrapper<ID, JOIN_ID, K, V> build()
	{
		JoinWideMapWrapper<ID, JOIN_ID, K, V> wrapper = new JoinWideMapWrapper<ID, JOIN_ID, K, V>();
		build(wrapper);
		wrapper.setLoader(loader);
		wrapper.setPersister(persister);
		return wrapper;
	}

}
