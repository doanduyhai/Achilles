package info.archinnov.achilles.holder.factory;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.holder.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import com.google.common.collect.Lists;


/**
 * KeyValueFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueFactory
{
	private EntityLoader loader = new EntityLoader();

	private CompositeTransformer compositeTransformer = new CompositeTransformer();
	private DynamicCompositeTransformer dynamicCompositeTransformer = new DynamicCompositeTransformer();

	// Dynamic Composite
	public <K, V> KeyValue<K, V> createKeyValueForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		return dynamicCompositeTransformer.buildKeyValueFromDynamicComposite(propertyMeta, hColumn);
	}

	public <K, V> K createKeyForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		return dynamicCompositeTransformer.buildKeyFromDynamicComposite(propertyMeta, hColumn);
	}

	public <K, V> V createValueForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		return propertyMeta.getValue(hColumn.getValue());
	}

	public <K, V> List<V> createValueListForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, Object>> hColumns)
	{
		return Lists.transform(hColumns,
				dynamicCompositeTransformer.buildValueTransformer(propertyMeta));
	}

	@SuppressWarnings("unchecked")
	public <K, V> List<V> createJoinValueListForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, Object>> hColumns)
	{
		List<?> joinIds = Lists.transform(hColumns,
				dynamicCompositeTransformer.buildRawValueTransformer());
		Map<?, V> joinEntities = loader.loadJoinEntities(propertyMeta.getValueClass(), joinIds,
				propertyMeta.getJoinProperties().getEntityMeta());
		List<V> result = new ArrayList<V>();
		for (Object joinId : joinIds)
		{
			result.add(joinEntities.get(joinId));
		}

		return result;
	}

	public <K, V> List<K> createKeyListForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, Object>> hColumns)
	{
		return Lists.transform(hColumns,
				dynamicCompositeTransformer.buildKeyTransformer(propertyMeta));
	}

	public <K, V> List<KeyValue<K, V>> createKeyValueListForDynamicComposite(
			PropertyMeta<K, V> propertyMeta, List<HColumn<DynamicComposite, Object>> hColumns)
	{
		return Lists.transform(hColumns,
				dynamicCompositeTransformer.buildKeyValueTransformer(propertyMeta));
	}

	@SuppressWarnings("unchecked")
	public <K, V> List<KeyValue<K, V>> createJoinKeyValueListForDynamicComposite(
			PropertyMeta<K, V> propertyMeta, List<HColumn<DynamicComposite, Object>> hColumns)
	{
		List<K> keys = Lists.transform(hColumns,
				dynamicCompositeTransformer.buildKeyTransformer(propertyMeta));
		List<Object> joinIds = Lists.transform(hColumns,
				dynamicCompositeTransformer.buildRawValueTransformer());
		Map<Object, V> joinEntities = loader.loadJoinEntities(propertyMeta.getValueClass(),
				joinIds, propertyMeta.getJoinProperties().getEntityMeta());
		List<Integer> ttls = Lists.transform(hColumns,
				dynamicCompositeTransformer.buildTtlTransformer());

		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();

		for (int i = 0; i < keys.size(); i++)
		{
			result.add(new KeyValue<K, V>(keys.get(i), joinEntities.get(joinIds.get(i)), ttls
					.get(i)));
		}
		return result;
	}

	// Composite

	public <K, V> KeyValue<K, V> createKeyValueForComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, ?> hColumn)
	{
		return compositeTransformer.buildKeyValueFromComposite(propertyMeta, hColumn);
	}

	public <K, V> K createKeyForComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, ?> hColumn)
	{
		return compositeTransformer.buildKeyFromComposite(propertyMeta, hColumn);
	}

	public <K, V> V createValueForComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, ?> hColumn)
	{
		return propertyMeta.getValue(hColumn.getValue());
	}

	public <K, V> List<V> createValueListForComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, ?>> hColumns)
	{
		return Lists.transform(hColumns, compositeTransformer.buildValueTransformer(propertyMeta));
	}

	public <K, V> List<K> createKeyListForComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, ?>> hColumns)
	{
		return Lists.transform(hColumns, compositeTransformer.buildKeyTransformer(propertyMeta));
	}

	@SuppressWarnings("unchecked")
	public <K, V> List<V> createJoinValueListForComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, ?>> hColumns)
	{
		List<?> joinIds = Lists
				.transform(hColumns, compositeTransformer.buildRawValueTransformer());
		Map<?, V> joinEntities = loader.loadJoinEntities(propertyMeta.getValueClass(), joinIds,
				propertyMeta.getJoinProperties().getEntityMeta());
		List<V> result = new ArrayList<V>();
		for (Object joinId : joinIds)
		{
			result.add(joinEntities.get(joinId));
		}

		return result;
	}

	public <K, V> List<KeyValue<K, V>> createKeyValueListForComposite(
			PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, ?>> hColumns)
	{
		return Lists.transform(hColumns,
				compositeTransformer.buildKeyValueTransformer(propertyMeta));
	}

	@SuppressWarnings("unchecked")
	public <K, V> List<KeyValue<K, V>> createJoinKeyValueListForComposite(
			PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, ?>> hColumns)
	{
		List<K> keys = Lists.transform(hColumns,
				compositeTransformer.buildKeyTransformer(propertyMeta));
		List<Object> joinIds = Lists.transform(hColumns,
				compositeTransformer.buildRawValueTransformer());
		Map<Object, V> joinEntities = loader.loadJoinEntities(propertyMeta.getValueClass(),
				joinIds, propertyMeta.getJoinProperties().getEntityMeta());
		List<Integer> ttls = Lists.transform(hColumns, compositeTransformer.buildTtlTransformer());

		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();

		for (int i = 0; i < keys.size(); i++)
		{
			result.add(new KeyValue<K, V>(keys.get(i), joinEntities.get(joinIds.get(i)), ttls
					.get(i)));
		}
		return result;
	}
}
