package fr.doan.achilles.wrapper.builder;

import fr.doan.achilles.dao.GenericWideRowDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.WideRowWrapper;

/**
 * InternalSingleKeyWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideRowWrapperBuilder<ID, K, V>
{
	private ID id;
	private GenericWideRowDao<ID, V> dao;
	private PropertyMeta<K, V> wideMapMeta;

	public WideRowWrapperBuilder(ID id, GenericWideRowDao<ID, V> dao, PropertyMeta<K, V> wideMapMeta)
	{
		this.id = id;
		this.dao = dao;
		this.wideMapMeta = wideMapMeta;
	}

	public static <ID, K, V> WideRowWrapperBuilder<ID, K, V> builder(ID id,
			GenericWideRowDao<ID, V> dao, PropertyMeta<K, V> wideMapMeta)
	{
		return new WideRowWrapperBuilder<ID, K, V>(id, dao, wideMapMeta);
	}

	public WideRowWrapper<ID, K, V> build()
	{
		WideRowWrapper<ID, K, V> wrapper = new WideRowWrapper<ID, K, V>();
		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setWideMapMeta(wideMapMeta);
		return wrapper;
	}

}
