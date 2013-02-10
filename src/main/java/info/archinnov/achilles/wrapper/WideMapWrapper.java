package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.holder.factory.KeyValueFactory;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * WideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapper<ID, K, V> extends AbstractWideMapWrapper<K, V>
{

	protected ID id;
	protected GenericDynamicCompositeDao<ID> dao;
	protected PropertyMeta<K, V> wideMapMeta;
	protected CompositeHelper helper = new CompositeHelper();
	protected KeyValueFactory keyValueFactory = new KeyValueFactory();
	protected IteratorFactory iteratorFactory = new IteratorFactory();
	protected DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	protected DynamicComposite buildComposite(K key)
	{
		return keyFactory.createForInsert(wideMapMeta, key);
	}

	@Override
	public V get(K key)
	{
		Object value = dao.getValue(id, buildComposite(key));
		return wideMapMeta.getValueFromString(value);
	}

	@Override
	public void insert(K key, V value, int ttl)
	{

		dao.setValue(id, buildComposite(key), wideMapMeta.writeValueToString(value), ttl);
	}

	@Override
	public void insert(K key, V value)
	{
		dao.setValue(id, buildComposite(key), wideMapMeta.writeValueToString(value));
	}

	@Override
	public List<KeyValue<K, V>> find(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{

		helper.checkBounds(wideMapMeta, start, end, reverse);

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, String>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		if (wideMapMeta.type().isJoinColumn())
		{
			return keyValueFactory.createJoinKeyValueListForDynamicComposite(wideMapMeta, hColumns);
		}
		else
		{

			return keyValueFactory.createKeyValueListForDynamicComposite(wideMapMeta, hColumns);
		}
	}

	@Override
	public List<V> findValues(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{

		helper.checkBounds(wideMapMeta, start, end, reverse);

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, String>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);
		if (wideMapMeta.type().isJoinColumn())
		{
			return keyValueFactory.createJoinValueListForDynamicComposite(wideMapMeta, hColumns);
		}
		else
		{

			return keyValueFactory.createValueListForDynamicComposite(wideMapMeta, hColumns);
		}
	}

	@Override
	public List<K> findKeys(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{

		helper.checkBounds(wideMapMeta, start, end, reverse);

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, String>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);
		return keyValueFactory.createKeyListForDynamicComposite(wideMapMeta, hColumns);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		if (wideMapMeta.type().isJoinColumn())
		{

			AchillesJoinSliceIterator<ID, DynamicComposite, String, K, V> joinColumnSliceIterator = dao
					.getJoinColumnsIterator(wideMapMeta, id, queryComps[0], queryComps[1], reverse,
							count);

			return iteratorFactory.createKeyValueJoinIteratorForDynamicComposite(
					joinColumnSliceIterator, wideMapMeta);

		}
		else
		{

			AchillesSliceIterator<ID, DynamicComposite, String> columnSliceIterator = dao
					.getColumnsIterator(id, queryComps[0], queryComps[1], reverse, count);

			return iteratorFactory.createKeyValueIteratorForDynamicComposite(columnSliceIterator,
					wideMapMeta);
		}
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

		DynamicComposite[] queryComps = keyFactory.createForQuery(//
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, false);

		dao.removeColumnRange(id, queryComps[0], queryComps[1]);
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

	public void setDao(GenericDynamicCompositeDao<ID> dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.wideMapMeta = wideMapMeta;
	}

}
