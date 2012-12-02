package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.entity.type.KeyValue;
import fr.doan.achilles.entity.type.KeyValueIterator;

/**
 * WideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapper<ID, K, V>
{
	private ID id;
	private GenericDao<ID> dao;
	private WideMapMeta<K, V> wideMapMeta;

	public V getValue(K key)
	{
		return null;
	}

	public void insertValue(K key, V value, int ttl)
	{
		DynamicComposite composite = dao.buildComponentForProperty(wideMapMeta.getPropertyName(),
				PropertyType.WIDE_MAP, key, wideMapMeta.getKeySerializer());
		dao.setValue(id, composite, (Object) value, ttl);
	}

	public void insertValue(K key, V value)
	{
		DynamicComposite composite = dao.buildComponentForProperty(wideMapMeta.getPropertyName(),
				PropertyType.WIDE_MAP, key, wideMapMeta.getKeySerializer());
		dao.setValue(id, composite, (Object) value);
	}

	public List<KeyValue<K, V>> findValues(K start, K end, boolean reverse, int count)
	{
		return null;
	}

	public List<KeyValue<K, V>> findValues(K start, K end, boolean inclusiveBounds,
			boolean reverse, int count)
	{
		return null;
	}

	public List<KeyValue<K, V>> findValues(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{
		return null;
	}

	public KeyValueIterator<K, V> iterator(K start, K end, boolean reverse, int count)
	{
		return null;
	}

	public KeyValueIterator<K, V> iterator(K start, K end, boolean inclusiveBounds,
			boolean reverse, int count)
	{
		return null;
	}

	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{
		return null;
	}

	public void removeValue(K key)
	{

	}

	public void removeValues(K start, K end)
	{

	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setDao(GenericDao<ID> dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(WideMapMeta<K, V> wideMapMeta)
	{
		this.wideMapMeta = wideMapMeta;
	}
}
