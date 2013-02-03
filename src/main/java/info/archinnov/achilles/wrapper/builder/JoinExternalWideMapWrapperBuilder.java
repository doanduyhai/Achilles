package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
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

	public JoinExternalWideMapWrapper<ID, JOIN_ID, K, V> build()
	{
		JoinExternalWideMapWrapper<ID, JOIN_ID, K, V> wrapper = new JoinExternalWideMapWrapper<ID, JOIN_ID, K, V>();

		wrapper.setId(id);
		wrapper.setExternalWideMapMeta(joinExternalWideMapMeta);
		wrapper.setExternalWideMapDao(dao);

		return wrapper;
	}

}
