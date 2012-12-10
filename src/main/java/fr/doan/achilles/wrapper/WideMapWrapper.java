package fr.doan.achilles.wrapper;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.entity.type.KeyValue;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.validation.Validator;
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
	protected GenericDao<ID> dao;
	protected WideMapMeta<K, V> wideMapMeta;

	protected DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	@Override
	public V getValue(K key)
	{
		DynamicComposite composite = buildComposite(key);
		Object value = dao.getValue(id, composite);

		return wideMapMeta.get(value);
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

		validateBounds(start, end, reverse);

		DynamicComposite[] queryComps = buildQueryComposites(start, inclusiveStart, end,
				inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, Object>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return KeyValue.fromList(hColumns, wideMapMeta.getKeySerializer(), wideMapMeta);
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

		validateBounds(start, end, false);

		DynamicComposite[] queryComps = buildQueryComposites(start, inclusiveStart, end,
				inclusiveEnd, false);

		dao.removeColumnRange(id, queryComps[0], queryComps[1]);
	}

	protected DynamicComposite buildComposite(K key)
	{
		return keyFactory.buildForProperty(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), key, wideMapMeta.getKeySerializer());
	}

	protected DynamicComposite buildQueryComposite(K value, ComponentEquality equality)
	{
		return keyFactory.buildQueryComparator(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), value, equality);
	}

	@SuppressWarnings("unchecked")
	protected void validateBounds(K start, K end, boolean reverse)
	{

		if (start != null && end != null)
		{
			Comparable<K> startComp = (Comparable<K>) start;

			if (reverse)
			{
				Validator
						.validateTrue(startComp.compareTo(end) >= 0,
								"For reverse range query, start value should be greater or equal to end value");
			}
			else
			{
				Validator.validateTrue(startComp.compareTo(end) <= 0,
						"For range query, start value should be lesser or equal to end value");
			}
		}
	}

	protected ComponentEquality[] determineEquality(boolean inclusiveStart, boolean inclusiveEnd,
			boolean reverse)
	{
		ComponentEquality[] result = new ComponentEquality[2];
		ComponentEquality start;
		ComponentEquality end;
		if (reverse)
		{
			start = inclusiveStart ? GREATER_THAN_EQUAL : LESS_THAN_EQUAL;
			end = inclusiveEnd ? EQUAL : GREATER_THAN_EQUAL;
		}
		else
		{
			start = inclusiveStart ? EQUAL : GREATER_THAN_EQUAL;
			end = inclusiveEnd ? GREATER_THAN_EQUAL : LESS_THAN_EQUAL;
		}

		result[0] = start;
		result[1] = end;
		return result;
	}

	protected DynamicComposite[] buildQueryComposites(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse)
	{
		DynamicComposite[] queryComp = new DynamicComposite[2];

		ComponentEquality[] equalities = determineEquality(inclusiveStart, inclusiveEnd, reverse);
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

	public void setDao(GenericDao<ID> dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(WideMapMeta<K, V> wideMapMeta)
	{
		this.wideMapMeta = wideMapMeta;
	}
}
