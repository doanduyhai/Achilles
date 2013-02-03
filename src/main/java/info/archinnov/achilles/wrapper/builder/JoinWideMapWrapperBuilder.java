package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.JoinWideMapWrapper;

/**
 * JoinWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapWrapperBuilder<ID, K, V> extends WideMapWrapperBuilder<ID, K, V>
{

	public JoinWideMapWrapperBuilder(ID id, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<K, V> joinWideMapMeta)
	{
		super(id, dao, joinWideMapMeta);
	}

	public static <ID, K, V> JoinWideMapWrapperBuilder<ID, K, V> builder(ID id,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<K, V> wideMapMeta)
	{
		return new JoinWideMapWrapperBuilder<ID, K, V>(id, dao, wideMapMeta);
	}

	public JoinWideMapWrapper<ID, K, V> build()
	{
		JoinWideMapWrapper<ID, K, V> wrapper = new JoinWideMapWrapper<ID, K, V>();
		build(wrapper);
		return wrapper;
	}

}
