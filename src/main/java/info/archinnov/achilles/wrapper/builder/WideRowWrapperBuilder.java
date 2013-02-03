package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.ExternalWideMapWrapper;

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

	public ExternalWideMapWrapper<ID, K, V> build()
	{
		ExternalWideMapWrapper<ID, K, V> wrapper = new ExternalWideMapWrapper<ID, K, V>();
		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setWideMapMeta(wideMapMeta);
		return wrapper;
	}

}
