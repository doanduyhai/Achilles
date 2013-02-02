package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.composite.factory.CompositeKeyFactory;
import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.AchillesSliceIterator;
import fr.doan.achilles.iterator.factory.IteratorFactory;

/**
 * WideRowWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ExternalWideMapWrapper<ID, K, V> extends AbstractWideMapWrapper<K, V>
{
	protected ID id;
	protected GenericCompositeDao<ID, V> dao;
	protected PropertyMeta<K, V> wideMapMeta;

	protected CompositeHelper helper = new CompositeHelper();
	protected KeyValueFactory keyValueFactory = new KeyValueFactory();
	protected IteratorFactory iteratorFactory = new IteratorFactory();
	protected CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory();

	protected Composite buildComposite(K key)
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

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Override
	public List<KeyValue<K, V>> find(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{
		helper.checkBounds(wideMapMeta, start, end, reverse);

		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0],
				composites[1], reverse, count);

		return keyValueFactory.createKeyValueListForComposite(wideMapMeta, (List) hColumns);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public List<V> findValues(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{
		helper.checkBounds(wideMapMeta, start, end, reverse);

		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0],
				composites[1], reverse, count);

		return keyValueFactory.createValueListForComposite(wideMapMeta, (List) hColumns);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public List<K> findKeys(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{
		helper.checkBounds(wideMapMeta, start, end, reverse);

		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0],
				composites[1], reverse, count);

		return keyValueFactory.createKeyListForComposite(wideMapMeta, (List) hColumns);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		AchillesSliceIterator<ID, Composite, V> columnSliceIterator = dao.getColumnsIterator(id,
				composites[0], composites[1], reverse, count);

		return iteratorFactory.createKeyValueIteratorForComposite(columnSliceIterator, wideMapMeta);

	}

	@Override
	public void remove(K key)
	{
		dao.removeColumn(id, buildComposite(key));
	}

	@Override
	public void remove(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{
		helper.checkBounds(wideMapMeta, start, end, false);
		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, false);
		dao.removeColumnRange(id, composites[0], composites[1]);
	}

	@Override
	public void removeFirst(int count)
	{
		dao.removeColumnRange(id, null, null, false, count);

	}

	@Override
	public void removeLast(int count)
	{
		dao.removeColumnRange(id, null, null, true, count);
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setDao(GenericCompositeDao<ID, V> dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.wideMapMeta = wideMapMeta;
	}

}
