package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * WideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapper<ID, K, V> extends AbstractWideMapWrapper<K, V>
{

	private CompositeHelper helper = new CompositeHelper();
	private KeyValueFactory keyValueFactory = new KeyValueFactory();
	private IteratorFactory iteratorFactory = new IteratorFactory();
	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	protected ID id;
	protected GenericDynamicCompositeDao<ID> entityDao;
	protected GenericCompositeDao<ID, ?> columnFamilyDao;
	protected PropertyMeta<K, V> wideMapMeta;

	protected DynamicComposite buildComposite(K key)
	{
		return keyFactory.createForInsert(wideMapMeta, key);
	}

	@Override
	public V get(K key)
	{
		Object value = entityDao.getValue(id, buildComposite(key));
		return wideMapMeta.getValueFromString(value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value, int ttl)
	{
		if (this.interceptor.isBatchMode())
		{
			entityDao.setValueBatch(id, buildComposite(key), wideMapMeta.writeValueToString(value),
					ttl, (Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			entityDao.setValue(id, buildComposite(key), wideMapMeta.writeValueToString(value), ttl);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value)
	{
		if (this.interceptor.isBatchMode())
		{
			entityDao.setValueBatch(id, buildComposite(key), wideMapMeta.writeValueToString(value),
					(Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			entityDao.setValue(id, buildComposite(key), wideMapMeta.writeValueToString(value));
		}
	}

	@Override
	public List<KeyValue<K, V>> find(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{

		helper.checkBounds(wideMapMeta, start, end, reverse);

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, String>> hColumns = entityDao.findRawColumnsRange(id,
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

		List<HColumn<DynamicComposite, String>> hColumns = entityDao.findRawColumnsRange(id,
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

		List<HColumn<DynamicComposite, String>> hColumns = entityDao.findRawColumnsRange(id,
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

			AchillesJoinSliceIterator<ID, DynamicComposite, String, K, V> joinColumnSliceIterator = entityDao
					.getJoinColumnsIterator(wideMapMeta, id, queryComps[0], queryComps[1], reverse,
							count);

			return iteratorFactory.createKeyValueJoinIteratorForDynamicComposite(
					joinColumnSliceIterator, wideMapMeta);

		}
		else
		{

			AchillesSliceIterator<ID, DynamicComposite, String> columnSliceIterator = entityDao
					.getColumnsIterator(id, queryComps[0], queryComps[1], reverse, count);

			return iteratorFactory.createKeyValueIteratorForDynamicComposite(columnSliceIterator,
					wideMapMeta);
		}
	}

	@Override
	public void remove(K key)
	{
		entityDao.removeColumn(id, buildComposite(key));
	}

	@Override
	public void remove(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{

		helper.checkBounds(wideMapMeta, start, end, false);

		DynamicComposite[] queryComps = keyFactory.createForQuery(//
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, false);

		entityDao.removeColumnRange(id, queryComps[0], queryComps[1]);
	}

	@Override
	public void removeFirst(int count)
	{
		entityDao.removeColumnRange(id, null, null, false, count);

	}

	@Override
	public void removeLast(int count)
	{
		entityDao.removeColumnRange(id, null, null, true, count);
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setEntityDao(GenericDynamicCompositeDao<ID> entityDao)
	{
		this.entityDao = entityDao;
	}

	public void setColumnFamilyDao(GenericCompositeDao<ID, ?> columnFamilyDao)
	{
		this.columnFamilyDao = columnFamilyDao;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.wideMapMeta = wideMapMeta;
	}

}
