package fr.doan.achilles.wrapper;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.iterator.MultiKeyKeyValueIteratorForEntity;
import fr.doan.achilles.proxy.EntityWrapperUtil;

/**
 * MultiKeyWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWideMapWrapper<ID, K, V> extends AbstractWideMapWrapper<ID, K, V>
{

	private EntityWrapperUtil util = new EntityWrapperUtil();
	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	private List<Serializer<?>> componentSerializers;
	private List<Method> componentGetters;
	private List<Method> componentSetters;

	@Override
	protected DynamicComposite buildComposite(K key)
	{

		List<Object> componentValues = util.determineMultiKey(key, componentGetters);

		return keyFactory.createForInsertMultiKey(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers);

	}

	@Override
	public List<KeyValue<K, V>> findRange(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		helper.checkMultiKeyBounds(componentGetters, wideMapMeta, start, end, reverse);

		DynamicComposite[] queryComps = keyFactory.createForMultiKeyQuery( //
				wideMapMeta.getPropertyName(), wideMapMeta.propertyType(), //
				componentSerializers, //
				componentGetters, //
				start, //
				inclusiveStart, //
				end, //
				inclusiveEnd, //
				reverse);

		List<HColumn<DynamicComposite, Object>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return util.buildMultiKeyListForDynamicComposite(wideMapMeta.getKeyClass(),
				(MultiKeyWideMapMeta<K, V>) wideMapMeta, hColumns, componentSetters);
	}

	public MultiKeyKeyValueIteratorForEntity<K, V> iterator(K start, K end, boolean reverse,
			int count)
	{
		return this.iterator(start, end, true, reverse, count);
	}

	@Override
	public MultiKeyKeyValueIteratorForEntity<K, V> iterator(K start, K end,
			boolean inclusiveBounds, boolean reverse, int count)
	{
		return this.iterator(start, inclusiveBounds, end, inclusiveBounds, reverse, count);
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

		DynamicComposite[] queryComps = keyFactory.createForMultiKeyQuery( //
				wideMapMeta.getPropertyName(), wideMapMeta.propertyType(), //
				componentSerializers, //
				componentGetters, //
				start, //
				inclusiveStart, //
				end, //
				inclusiveEnd, //
				reverse);

		ColumnSliceIterator<ID, DynamicComposite, Object> columnSliceIterator = dao
				.getColumnsIterator(id, queryComps[0], queryComps[1], reverse, count);

		return new MultiKeyKeyValueIteratorForEntity(columnSliceIterator, componentSetters,
				(MultiKeyWideMapMeta<K, V>) wideMapMeta);
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

	@Override
	public void removeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{

		helper.checkMultiKeyBounds(componentGetters, wideMapMeta, start, end, false);

		DynamicComposite[] queryComps = keyFactory.createForMultiKeyQuery( //
				wideMapMeta.getPropertyName(), wideMapMeta.propertyType(), //
				componentSerializers, //
				componentGetters, //
				start, //
				inclusiveStart, //
				end, //
				inclusiveEnd, //
				false);

		dao.removeColumnRange(id, queryComps[0], queryComps[1]);
	}
}
