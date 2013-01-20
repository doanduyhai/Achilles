package fr.doan.achilles.wrapper.builder;

import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.JoinExternalWideRowWrapper;

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

	public JoinExternalWideRowWrapper<ID, JOIN_ID, K, V> build()
	{
		JoinExternalWideRowWrapper<ID, JOIN_ID, K, V> wrapper = new JoinExternalWideRowWrapper<ID, JOIN_ID, K, V>();

		wrapper.setId(id);
		wrapper.setExternalWideMapMeta(joinExternalWideMapMeta);
		wrapper.setExternalWideMapDao(dao);

		return wrapper;
	}

}
