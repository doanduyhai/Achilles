package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericDynamicCompositeDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.AchillesJoinSliceIterator;
import fr.doan.achilles.iterator.AchillesSliceIterator;
import fr.doan.achilles.iterator.factory.IteratorFactory;

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
	public List<KeyValue<K, V>> findRange(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		helper.checkBounds(wideMapMeta, start, end, reverse);

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, Object>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return keyValueFactory.createListForDynamicComposite(wideMapMeta, hColumns);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		if (wideMapMeta.type().isJoinColumn())
		{

			AchillesJoinSliceIterator<ID, DynamicComposite, Object, K, V> joinColumnSliceIterator = dao
					.getJoinColumnsIterator(wideMapMeta, id, queryComps[0], queryComps[1], reverse,
							count);

			return iteratorFactory.createKeyValueJoinIteratorForDynamicComposite(
					joinColumnSliceIterator, wideMapMeta);

		}
		else
		{

			AchillesSliceIterator<ID, DynamicComposite, Object> columnSliceIterator = dao
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
	public void removeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{

		helper.checkBounds(wideMapMeta, start, end, false);

		DynamicComposite[] queryComps = keyFactory.createForQuery(//
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, false);

		dao.removeColumnRange(id, queryComps[0], queryComps[1]);
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
