package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.composite.factory.CompositeKeyFactory;
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
	private GenericWideRowDao<ID> dao;
	private PropertyMeta<K, V> wideMapMeta;

	private CompositeHelper helper = new CompositeHelper();
	private KeyValueFactory keyValueFactory = new KeyValueFactory();
	private IteratorFactory iteratorFactory = new IteratorFactory();
	private CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory();

	private Composite buildComposite(K key)
	{
		Composite comp = compositeKeyFactory.createBaseComposite(wideMapMeta, key);
		return comp;
	}

	@Override
	public V get(K key)
	{
		Object value = dao.getValue(id, buildComposite(key));
		return wideMapMeta.getValue(value);
	}

	@Override
	public void insert(K key, V value)
	{
		dao.setValue(id, buildComposite(key), value);
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		dao.setValue(id, buildComposite(key), value, ttl);
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
		helper.checkBounds(wideMapMeta, start, end, reverse);

		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, Object>> hColumns = dao.findRawColumnsRange(id, composites[0],
				composites[1], reverse, count);

		return keyValueFactory.createListForWideRow(wideMapMeta, hColumns);
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

		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		ColumnSliceIterator<ID, Composite, Object> columnSliceIterator = dao.getColumnsIterator(id,
				composites[0], composites[1], reverse, count);

		return iteratorFactory.createKeyValueIteratorForWideRow(columnSliceIterator, wideMapMeta);

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

	@Override
	public void removeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{
		helper.checkBounds(wideMapMeta, start, end, false);
		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, false);
		dao.removeColumnRange(id, composites[0], composites[1]);
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setDao(GenericWideRowDao<ID> dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.wideMapMeta = wideMapMeta;
	}
}
