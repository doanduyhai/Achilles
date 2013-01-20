package fr.doan.achilles.wrapper.builder;

import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.ExternalWideRowWrapper;

/**
 * WideRowWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideRowWrapperBuilder<ID, K, V>
{
	private ID id;
	private GenericCompositeDao<ID, V> dao;
	private PropertyMeta<K, V> wideMapMeta;

	public WideRowWrapperBuilder(ID id, GenericCompositeDao<ID, V> dao,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.id = id;
		this.dao = dao;
		this.wideMapMeta = wideMapMeta;
	}

	public static <ID, K, V> WideRowWrapperBuilder<ID, K, V> builder(ID id,
			GenericCompositeDao<ID, V> dao, PropertyMeta<K, V> wideMapMeta)
	{
		return new WideRowWrapperBuilder<ID, K, V>(id, dao, wideMapMeta);
	}

	public ExternalWideRowWrapper<ID, K, V> build()
	{
		ExternalWideRowWrapper<ID, K, V> wrapper = new ExternalWideRowWrapper<ID, K, V>();
		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setWideMapMeta(wideMapMeta);
		return wrapper;
	}

}
