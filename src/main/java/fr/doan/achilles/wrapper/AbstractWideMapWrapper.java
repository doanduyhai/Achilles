package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.DynamicCompositeKeyValueIterator;
import fr.doan.achilles.iterator.factory.IteratorFactory;

/**
 * AbstractWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AbstractWideMapWrapper<ID, K, V> implements WideMap<K, V>
{
	protected ID id;
	protected GenericEntityDao<ID> dao;
	protected PropertyMeta<K, V> wideMapMeta;
	protected CompositeHelper helper = new CompositeHelper();
	protected KeyValueFactory keyValueFactory = new KeyValueFactory();
	protected IteratorFactory iteratorFactory = new IteratorFactory();

	@Override
	public V get(K key)
	{
		Object value = dao.getValue(id, buildComposite(key));

		return wideMapMeta.getValue(value);
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		dao.setValue(id, buildComposite(key), (Object) value, ttl);
	}

	@Override
	public void insert(K key, V value)
	{
		dao.setValue(id, buildComposite(key), (Object) value);
	}

	@Override
	public List<KeyValue<K, V>> findRange(K start, K end, boolean reverse, int count)
	{
		return findRange(start, end, true, reverse, count);
	}

	@Override
	public List<KeyValue<K, V>> findRange(K start, K end, boolean inclusiveBounds, boolean reverse,
			int count)
	{
		return findRange(start, inclusiveBounds, end, inclusiveBounds, reverse, count);
	}

	protected abstract DynamicComposite buildComposite(K key);

	@Override
	public DynamicCompositeKeyValueIterator<K, V> iterator(K start, K end, boolean reverse,
			int count)
	{
		return iterator(start, end, true, reverse, count);
	}

	@Override
	public DynamicCompositeKeyValueIterator<K, V> iterator(K start, K end, boolean inclusiveBounds,
			boolean reverse, int count)
	{
		return iterator(start, inclusiveBounds, end, inclusiveBounds, reverse, count);
	}

	@Override
	public void remove(K key)
	{
		dao.removeColumn(id, buildComposite(key));
	}

	@Override
	public void removeRange(K start, K end)
	{
		removeRange(start, end, true);
	}

	@Override
	public void removeRange(K start, K end, boolean inclusiveBounds)
	{
		removeRange(start, inclusiveBounds, end, inclusiveBounds);
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setDao(GenericEntityDao<ID> dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.wideMapMeta = wideMapMeta;
	}
}
