package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.metadata.PropertyType;
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
public class WideMapWrapper<ID, K extends Comparable<K>, V> implements WideMap<K, V>
{
	private ID id;
	private GenericDao<ID> dao;
	private WideMapMeta<K, V> wideMapMeta;

	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	@Override
	public V getValue(K key)
	{
		DynamicComposite composite = keyFactory.buildForProperty(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), key, wideMapMeta.getKeySerializer());
		Object value = dao.getValue(id, composite);

		return wideMapMeta.get(value);
	}

	@Override
	public void insertValue(K key, V value, int ttl)
	{
		DynamicComposite composite = keyFactory.buildForProperty(wideMapMeta.getPropertyName(),
				PropertyType.WIDE_MAP, key, wideMapMeta.getKeySerializer());
		dao.setValue(id, composite, (Object) value, ttl);
	}

	@Override
	public void insertValue(K key, V value)
	{
		DynamicComposite composite = keyFactory.buildForProperty(wideMapMeta.getPropertyName(),
				PropertyType.WIDE_MAP, key, wideMapMeta.getKeySerializer());
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
		if (start != null && end != null)
		{
			if (reverse)
			{
				Validator.validateTrue(start.compareTo(end) > 0,
						"For reverse range query, start value should be greater than end value");
			}
			else
			{
				Validator.validateTrue(start.compareTo(end) < 0,
						"For range query, start value should be lesser than end value");
			}
		}

		DynamicComposite startComp = keyFactory.buildQueryComparatorStart(
				wideMapMeta.getPropertyName(), wideMapMeta.propertyType(), start, inclusiveStart);
		DynamicComposite endComp = keyFactory.buildQueryComparatorEnd(
				wideMapMeta.getPropertyName(), wideMapMeta.propertyType(), end, inclusiveEnd);

		List<HColumn<DynamicComposite, Object>> hColumns = dao.findRawColumnsRange(id, startComp,
				endComp, reverse, count);

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
		DynamicComposite startComp = keyFactory.buildQueryComparatorStart(
				wideMapMeta.getPropertyName(), wideMapMeta.propertyType(), start, inclusiveStart);
		DynamicComposite endComp = keyFactory.buildQueryComparatorEnd(
				wideMapMeta.getPropertyName(), wideMapMeta.propertyType(), end, inclusiveEnd);

		ColumnSliceIterator<ID, DynamicComposite, Object> columnSliceIterator = dao
				.getColumnsIterator(id, startComp, endComp, reverse, count);

		return new KeyValueIterator(columnSliceIterator, wideMapMeta.getKeySerializer());
	}

	@Override
	public void removeValue(K key)
	{
		DynamicComposite comp = keyFactory.buildForProperty(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), key, wideMapMeta.getKeySerializer());

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

		if (start != null && end != null)
		{
			Validator.validateTrue(start.compareTo(end) < 0,
					"For range query, start value should be lesser than end value");
		}

		DynamicComposite startComp = keyFactory.buildQueryComparatorStart(
				wideMapMeta.getPropertyName(), wideMapMeta.propertyType(), start, inclusiveStart);
		DynamicComposite endComp = keyFactory.buildQueryComparatorEnd(
				wideMapMeta.getPropertyName(), wideMapMeta.propertyType(), end, inclusiveEnd);

		dao.removeColumnRange(id, startComp, endComp);
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
