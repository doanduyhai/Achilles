package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.dao.GenericWideRowDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.factory.IteratorFactory;

/**
 * WideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideRowWrapper<ID, K, V> implements WideMap<K, V>
{
	private ID id;
	private GenericWideRowDao<ID, K> dao;
	private PropertyMeta<K, V> wideMapMeta;

	private CompositeHelper helper = new CompositeHelper();
	private KeyValueFactory keyValueFactory = new KeyValueFactory();
	private IteratorFactory iteratorFactory = new IteratorFactory();

	@Override
	public V get(K key)
	{
		Object value = dao.getValue(id, key);
		return wideMapMeta.getValue(value);
	}

	@Override
	public void insert(K key, V value)
	{
		dao.setValue(id, key, (Object) value);
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		dao.setValue(id, key, (Object) value, ttl);
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

	@Override
	public List<KeyValue<K, V>> findRange(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{
		helper.checkBounds(start, end, reverse);

		List<HColumn<K, Object>> hColumns = dao.findRawColumnsRange(id, start, end, reverse, count);

		return keyValueFactory.createFromColumnList(hColumns, wideMapMeta.getKeySerializer(),
				wideMapMeta);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, boolean reverse, int count)
	{
		return iterator(start, end, true, reverse, count);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, boolean inclusiveBounds,
			boolean reverse, int count)
	{
		return iterator(start, inclusiveBounds, end, inclusiveBounds, reverse, count);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		ColumnSliceIterator<ID, K, Object> columnSliceIterator = dao.getColumnsIterator(id, start,
				end, reverse, count);

		return iteratorFactory.createKeyValueIteratorForWideRow(columnSliceIterator, wideMapMeta);
	}

	@Override
	public void remove(K key)
	{

		dao.removeColumn(id, key);
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

	@Override
	public void removeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{
		helper.checkBounds(start, end, false);
		dao.removeColumnRange(id, start, end);
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setDao(GenericWideRowDao<ID, K> dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.wideMapMeta = wideMapMeta;
	}
}
