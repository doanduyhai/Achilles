package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValue;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.wrapper.factory.DynamicCompositeKeyFactory;

/**
 * WideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapper<ID, K, V> implements WideMap<K, V>
{
	protected ID id;
	protected GenericEntityDao<ID> dao;
	protected PropertyMeta<K, V> wideMapMeta;

	protected DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();
	protected CompositeHelper helper = new CompositeHelper();

	@Override
	public V getValue(K key)
	{
		DynamicComposite composite = buildComposite(key);
		Object value = dao.getValue(id, composite);

		return wideMapMeta.getValue(value);
	}

	@Override
	public void insertValue(K key, V value, int ttl)
	{
		DynamicComposite composite = buildComposite(key);
		dao.setValue(id, composite, (Object) value, ttl);
	}

	@Override
	public void insertValue(K key, V value)
	{
		DynamicComposite composite = buildComposite(key);
		dao.setValue(id, composite, (Object) value);
	}

	@Override
	public List<KeyValue<K, V>> findValues(K start, K end, boolean reverse, int count)
	{
		return findValues(start, end, true, reverse, count);
	}

	@Override
	public List<KeyValue<K, V>> findValues(K start, K end, boolean inclusiveBounds,
			boolean reverse, int count)
	{
		return findValues(start, inclusiveBounds, end, inclusiveBounds, reverse, count);
	}

	@Override
	public List<KeyValue<K, V>> findValues(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		helper.validateBounds(start, end, reverse);

		DynamicComposite[] queryComps = buildQueryComposites(start, inclusiveStart, end,
				inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, Object>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return KeyValue.fromListOfDynamicCompositeHColums(hColumns, wideMapMeta.getKeySerializer(),
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
	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		DynamicComposite[] queryComps = buildQueryComposites(start, inclusiveStart, end,
				inclusiveEnd, reverse);

		ColumnSliceIterator<ID, DynamicComposite, Object> columnSliceIterator = dao
				.getColumnsIterator(id, queryComps[0], queryComps[1], reverse, count);

		return new KeyValueIterator(columnSliceIterator, wideMapMeta.getKeySerializer());
	}

	@Override
	public void removeValue(K key)
	{
		DynamicComposite comp = buildComposite(key);

		dao.removeColumn(id, comp);
	}

	@Override
	public void removeValues(K start, K end)
	{
		removeValues(start, end, true);
	}

	@Override
	public void removeValues(K start, K end, boolean inclusiveBounds)
	{
		removeValues(start, inclusiveBounds, end, inclusiveBounds);
	}

	@Override
	public void removeValues(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{

		helper.validateBounds(start, end, false);

		DynamicComposite[] queryComps = buildQueryComposites(start, inclusiveStart, end,
				inclusiveEnd, false);

		dao.removeColumnRange(id, queryComps[0], queryComps[1]);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	protected DynamicComposite buildComposite(K key)
	{
		return keyFactory.buildForInsert(wideMapMeta.getPropertyName(), wideMapMeta.propertyType(),
				key, (Serializer) wideMapMeta.getKeySerializer());
	}

	protected DynamicComposite buildQueryComposite(K value, ComponentEquality equality)
	{
		return keyFactory.buildQueryComparator(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), value, equality);
	}

	protected DynamicComposite[] buildQueryComposites(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse)
	{
		DynamicComposite[] queryComp = new DynamicComposite[2];

		ComponentEquality[] equalities = helper.determineEquality(inclusiveStart, inclusiveEnd,
				reverse);
		DynamicComposite startComp = buildQueryComposite(start, equalities[0]);
		DynamicComposite endComp = buildQueryComposite(end, equalities[1]);

		queryComp[0] = startComp;
		queryComp[1] = endComp;

		return queryComp;
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
