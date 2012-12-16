package fr.doan.achilles.wrapper.builder;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.WideMapWrapper;

/**
 * InternalSingleKeyWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapperBuilder<ID, K, V>
{
	private ID id;
	private GenericDao<ID> dao;
	private PropertyMeta<K, V> wideMapMeta;

	public WideMapWrapperBuilder(ID id, GenericDao<ID> dao, PropertyMeta<K, V> wideMapMeta) {
		this.id = id;
		this.dao = dao;
		this.wideMapMeta = wideMapMeta;
	}

	public static <ID, K, V> WideMapWrapperBuilder<ID, K, V> builder(ID id, GenericDao<ID> dao,
			PropertyMeta<K, V> wideMapMeta)
	{
		return new WideMapWrapperBuilder<ID, K, V>(id, dao, wideMapMeta);
	}

	public WideMapWrapper<ID, K, V> build()
	{
		WideMapWrapper<ID, K, V> wrapper = new WideMapWrapper<ID, K, V>();
		build(wrapper);
		return wrapper;
	}

	protected <T extends WideMapWrapper<ID, K, V>> void build(T wrapper)
	{
		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setWideMapMeta(wideMapMeta);
	}
}
