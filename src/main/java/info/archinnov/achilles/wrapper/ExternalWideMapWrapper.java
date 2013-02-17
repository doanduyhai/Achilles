package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.holder.factory.KeyValueFactory;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * ExternalWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings(
{
		"unchecked",
		"rawtypes"
})
public class ExternalWideMapWrapper<ID, K, V> extends AbstractWideMapWrapper<K, V>
{
	protected ID id;
	protected GenericCompositeDao dao;
	protected PropertyMeta<K, V> wideMapMeta;

	CompositeHelper compositeHelper = new CompositeHelper();
	KeyValueFactory keyValueFactory = new KeyValueFactory();
	IteratorFactory iteratorFactory = new IteratorFactory();
	CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory();

	protected Composite buildComposite(K key)
	{
		Composite comp = compositeKeyFactory.createBaseComposite(wideMapMeta, key);
		return comp;
	}

	@Override
	public V get(K key)
	{
		Object value = dao.getValue(id, buildComposite(key));
		return wideMapMeta.castValue(value);
	}

	@Override
	public void insert(K key, V value)
	{
		if (this.interceptor.isBatchMode())
		{
			dao.setValueBatch(id, buildComposite(key),
					wideMapMeta.writeValueAsSupportedTypeOrString(value),
					(Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			dao.setValue(id, buildComposite(key),
					wideMapMeta.writeValueAsSupportedTypeOrString(value));
		}
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		if (this.interceptor.isBatchMode())
		{
			dao.setValueBatch(id, buildComposite(key),
					wideMapMeta.writeValueAsSupportedTypeOrString(value), ttl,
					(Mutator<ID>) interceptor.getMutator());
		}
		else
		{

			dao.setValue(id, buildComposite(key),
					wideMapMeta.writeValueAsSupportedTypeOrString(value), ttl);
		}
	}

	@Override
	public List<KeyValue<K, V>> find(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{
		compositeHelper.checkBounds(wideMapMeta, start, end, reverse);

		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0],
				composites[1], reverse, count);

		return keyValueFactory.createKeyValueListForComposite(wideMapMeta, (List) hColumns);
	}

	public List<V> findValues(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{
		compositeHelper.checkBounds(wideMapMeta, start, end, reverse);

		Composite[] composites = compositeKeyFactory.createForQuery(wideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0],
				composites[1], reverse, count);

		return keyValueFactory.createValueListForComposite(wideMapMeta, (List) hColumns);
	}

	public List<K> findKeys(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{
		compositeHelper.checkBounds(wideMapMeta, start, end, reverse);

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
		compositeHelper.checkBounds(wideMapMeta, start, end, false);
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
