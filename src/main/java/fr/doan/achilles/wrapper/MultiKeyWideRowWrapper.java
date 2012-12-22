package fr.doan.achilles.wrapper;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.dao.GenericMultiKeyWideRowDao;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.iterator.MultiKeyKeyValueIteratorForEntity;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.validation.Validator;
import fr.doan.achilles.wrapper.factory.CompositeKeyFactory;

/**
 * WideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWideRowWrapper<ID, K, V> implements WideMap<K, V>
{
	private ID id;
	private GenericMultiKeyWideRowDao<ID> dao;
	private PropertyMeta<K, V> wideMapMeta;

	private List<Serializer<?>> componentSerializers;
	private List<Method> componentGetters;
	private List<Method> componentSetters;

	private EntityWrapperUtil util = new EntityWrapperUtil();
	private CompositeKeyFactory keyFactory = new CompositeKeyFactory();
	private CompositeHelper helper = new CompositeHelper();

	@Override
	public V get(K key)
	{
		Composite composite = buildCompositeForInsert(key);
		Object value = dao.getValue(id, composite);

		return wideMapMeta.getValue(value);
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		Composite composite = buildCompositeForInsert(key);
		dao.setValue(id, composite, (Object) value, ttl);
	}

	@Override
	public void insert(K key, V value)
	{
		Composite composite = buildCompositeForInsert(key);
		dao.setValue(id, composite, (Object) value);
	}

	@Override
	public List<KeyValue<K, V>> findRange(K start, K end, boolean reverse, int count)
	{
		return this.findRange(start, true, end, true, reverse, count);
	}

	@Override
	public List<KeyValue<K, V>> findRange(K start, K end, boolean inclusiveBounds, boolean reverse,
			int count)
	{
		return this.findRange(start, inclusiveBounds, end, inclusiveBounds, reverse, count);
	}

	@Override
	public List<KeyValue<K, V>> findRange(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		helper.checkMultiKeyBounds(componentGetters, wideMapMeta, start, end, reverse);

		Composite[] queryComps = buildQueryComposites(start, inclusiveStart, end, inclusiveEnd,
				reverse);

		List<HColumn<Composite, Object>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
				queryComps[1], reverse, count);

		return util.buildMultiKeyListForComposite(wideMapMeta.getKeyClass(),
				(MultiKeyWideMapMeta<K, V>) wideMapMeta, hColumns, componentSetters);
	}

	@Override
	public MultiKeyKeyValueIteratorForEntity<K, V> iterator(K start, K end, boolean reverse,
			int count)
	{
		return iterator(start, end, true, reverse, count);
	}

	@Override
	public MultiKeyKeyValueIteratorForEntity<K, V> iterator(K start, K end,
			boolean inclusiveBounds, boolean reverse, int count)
	{
		return iterator(start, inclusiveBounds, end, inclusiveBounds, reverse, count);
	}

	@Override
	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public MultiKeyKeyValueIteratorForEntity<K, V> iterator(K start, boolean inclusiveStart,
			K end, boolean inclusiveEnd, boolean reverse, int count)
	{

		Composite[] queryComps = buildQueryComposites(start, inclusiveStart, end, inclusiveEnd,
				reverse);

		ColumnSliceIterator<ID, Composite, Object> columnSliceIterator = dao.getColumnsIterator(id,
				queryComps[0], queryComps[1], reverse, count);

		return new MultiKeyKeyValueIteratorForEntity(columnSliceIterator, componentSetters,
				(MultiKeyWideMapMeta<K, V>) wideMapMeta);
	}

	@Override
	public void remove(K key)
	{
		Composite comp = buildCompositeForInsert(key);

		dao.removeColumn(id, comp);
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

		validateBounds(start, end, false);

		Composite[] queryComps = buildQueryComposites(start, inclusiveStart, end, inclusiveEnd,
				false);

		dao.removeColumnRange(id, queryComps[0], queryComps[1]);
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

	protected Composite[] buildQueryComposites(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse)
	{
		Composite[] queryComp = new Composite[2];

		ComponentEquality[] equalities = determineEquality(inclusiveStart, inclusiveEnd, reverse);
		Composite startComp = buildQueryComposite(start, equalities[0]);
		Composite endComp = buildQueryComposite(end, equalities[1]);

		queryComp[0] = startComp;
		queryComp[1] = endComp;

		return queryComp;
	}

	private Composite buildQueryComposite(K start, ComponentEquality equality)
	{
		List<Object> componentValues = util.determineMultiKey(start, componentGetters);

		return keyFactory.buildQueryComparator(wideMapMeta.getPropertyName(), componentValues,
				componentSerializers, equality);
	}

	private Composite buildCompositeForInsert(K key)
	{

		List<Object> componentValues = util.determineMultiKey(key, componentGetters);

		return keyFactory.buildForInsert(wideMapMeta.getPropertyName(), componentValues,
				componentSerializers);

	}

	@SuppressWarnings("unchecked")
	protected void validateBounds(K start, K end, boolean reverse)
	{

		if (start != null && end != null)
		{

			List<Object> startComponentValues = util.determineMultiKey(start, componentGetters);
			List<Object> endComponentValues = util.determineMultiKey(end, componentGetters);

			helper.findLastNonNullIndexForComponents(wideMapMeta.getPropertyName(),
					startComponentValues);
			helper.findLastNonNullIndexForComponents(wideMapMeta.getPropertyName(),
					endComponentValues);

			for (int i = 0; i < startComponentValues.size(); i++)
			{

				Comparable<Object> startValue = (Comparable<Object>) startComponentValues.get(i);
				Object endValue = endComponentValues.get(i);

				if (reverse)
				{
					if (startValue != null && endValue != null)
					{
						Validator
								.validateTrue(startValue.compareTo(endValue) >= 0,
										"For multiKey descending range query, startKey value should be greater or equal to end endKey");
					}

				}
				else
				{
					if (startValue != null && endValue != null)
					{
						Validator
								.validateTrue(startValue.compareTo(endValue) <= 0,
										"For multiKey ascending range query, startKey value should be lesser or equal to end endKey");
					}
				}
			}
		}
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setDao(GenericMultiKeyWideRowDao<ID> dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.wideMapMeta = wideMapMeta;
	}

	public void setComponentSerializers(List<Serializer<?>> componentSerializers)
	{
		this.componentSerializers = componentSerializers;
	}

	public void setComponentGetters(List<Method> componentGetters)
	{
		this.componentGetters = componentGetters;
	}

	public void setComponentSetters(List<Method> componentSetters)
	{
		this.componentSetters = componentSetters;
	}

}
