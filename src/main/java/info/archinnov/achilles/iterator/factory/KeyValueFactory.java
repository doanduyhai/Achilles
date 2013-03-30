package info.archinnov.achilles.iterator.factory;

import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.JoinEntityHelper;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import com.google.common.collect.Lists;

/**
 * KeyValueFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueFactory
{
	private JoinEntityHelper joinHelper = new JoinEntityHelper();
	private EntityProxifier proxifier = new EntityProxifier();
	private CompositeTransformer compositeTransformer = new CompositeTransformer();
	private DynamicCompositeTransformer dynamicCompositeTransformer = new DynamicCompositeTransformer();

	// Dynamic Composite
	public <ID, K, V> KeyValue<K, V> createKeyValueForDynamicComposite(
			PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, String> hColumn)
	{
		return dynamicCompositeTransformer.buildKeyValueFromDynamicComposite(context, propertyMeta,
				hColumn);
	}

	public <K, V> K createKeyForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, String> hColumn)
	{
		return dynamicCompositeTransformer.buildKeyFromDynamicComposite(propertyMeta, hColumn);
	}

	public <ID, K, V> V createValueForDynamicComposite(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<DynamicComposite, String> hColumn)
	{
		return dynamicCompositeTransformer.buildValueFromDynamicComposite(context, propertyMeta,
				hColumn);
	}

	public Integer createTtlForDynamicComposite(HColumn<DynamicComposite, ?> hColumn)
	{
		return hColumn.getTtl();
	}

	public <K, V> List<V> createValueListForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, String>> hColumns)
	{
		return Lists.transform(hColumns,
				dynamicCompositeTransformer.buildValueTransformer(propertyMeta));
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, K, V> List<V> createJoinValueListForDynamicComposite(
			PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, String>> hColumns)
	{
		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		List<JOIN_ID> joinIds = (List<JOIN_ID>) Lists.transform(hColumns,
				dynamicCompositeTransformer.buildRawValueTransformer(propertyMeta));

		Map<JOIN_ID, V> joinEntities = loadJoinEntities(context, propertyMeta, joinMeta, joinIds);
		List<V> result = new ArrayList<V>();
		for (Object joinId : joinIds)
		{
			V proxy = buildProxy(context, joinMeta, joinEntities, joinId);
			result.add(proxy);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, K, V> List<KeyValue<K, V>> createJoinKeyValueListForDynamicComposite(
			PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, String>> hColumns)
	{
		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		List<K> keys = Lists.transform(hColumns,
				dynamicCompositeTransformer.buildKeyTransformer(propertyMeta));
		List<JOIN_ID> joinIds = (List<JOIN_ID>) Lists.transform(hColumns,
				dynamicCompositeTransformer.buildRawValueTransformer(propertyMeta));

		Map<JOIN_ID, V> joinEntities = loadJoinEntities(context, propertyMeta, joinMeta, joinIds);
		List<Integer> ttls = Lists.transform(hColumns,
				dynamicCompositeTransformer.buildTtlTransformer());

		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();

		for (int i = 0; i < keys.size(); i++)
		{
			V proxy = buildProxy(context, joinMeta, joinEntities, joinIds.get(i));
			result.add(new KeyValue<K, V>(keys.get(i), proxy, ttls.get(i)));
		}
		return result;
	}

	public <K, V> List<K> createKeyListForDynamicComposite(PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, String>> hColumns)
	{
		return Lists.transform(hColumns,
				dynamicCompositeTransformer.buildKeyTransformer(propertyMeta));
	}

	public <ID, K, V> List<KeyValue<K, V>> createKeyValueListForDynamicComposite(
			PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, String>> hColumns)
	{
		return Lists.transform(hColumns,
				dynamicCompositeTransformer.buildKeyValueTransformer(context, propertyMeta));
	}

	// Composite
	public <ID, K, V> KeyValue<K, V> createKeyValueForComposite(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		return compositeTransformer.buildKeyValueFromComposite(context, propertyMeta, hColumn);
	}

	public <K, V> K createKeyForComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, ?> hColumn)
	{
		return compositeTransformer.buildKeyFromComposite(propertyMeta, hColumn);
	}

	public <ID, K, V> V createValueForComposite(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		return compositeTransformer.buildValueFromComposite(context, propertyMeta, hColumn);
	}

	public Integer createTtlForComposite(HColumn<Composite, ?> hColumn)
	{
		return hColumn.getTtl();
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

	public <ID, K, V> List<KeyValue<K, V>> createKeyValueListForComposite(
			PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, ?>> hColumns)
	{
		return Lists.transform(hColumns,
				compositeTransformer.buildKeyValueTransformer(context, propertyMeta));
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, K, V> List<V> createJoinValueListForComposite(
			PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, ?>> hColumns)
	{
		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		List<JOIN_ID> joinIds = (List<JOIN_ID>) Lists.transform(hColumns,
				compositeTransformer.buildRawValueTransformer());
		Map<JOIN_ID, V> joinEntities = loadJoinEntities(context, propertyMeta, joinMeta, joinIds);

		List<V> result = new ArrayList<V>();
		for (JOIN_ID joinId : joinIds)
		{
			V proxy = buildProxy(context, joinMeta, joinEntities, joinId);
			result.add(proxy);
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, K, V> List<KeyValue<K, V>> createJoinKeyValueListForComposite(
			PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, ?>> hColumns)
	{
		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		List<K> keys = Lists.transform(hColumns,
				compositeTransformer.buildKeyTransformer(propertyMeta));
		List<JOIN_ID> joinIds = (List<JOIN_ID>) Lists.transform(hColumns,
				compositeTransformer.buildRawValueTransformer());
		Map<JOIN_ID, V> joinEntities = loadJoinEntities(context, propertyMeta, joinMeta, joinIds);

		List<Integer> ttls = Lists.transform(hColumns, compositeTransformer.buildTtlTransformer());

		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();

		for (int i = 0; i < keys.size(); i++)
		{
			V proxy = buildProxy(context, joinMeta, joinEntities, joinIds.get(i));
			result.add(new KeyValue<K, V>(keys.get(i), proxy, ttls.get(i)));
		}
		return result;
	}

	// Counter
	public <K> KeyValue<K, Long> createCounterKeyValueForDynamicComposite(
			PropertyMeta<K, Long> propertyMeta, HCounterColumn<DynamicComposite> hColumn)
	{
		return dynamicCompositeTransformer.buildCounterKeyValueFromDynamicComposite(propertyMeta,
				hColumn);
	}

	public <K> K createCounterKeyForDynamicComposite(PropertyMeta<K, Long> propertyMeta,
			HCounterColumn<DynamicComposite> hColumn)
	{
		return dynamicCompositeTransformer.buildCounterKeyFromDynamicComposite(propertyMeta,
				hColumn);
	}

	public <K> Long createCounterValueForDynamicComposite(PropertyMeta<K, Long> propertyMeta,
			HCounterColumn<DynamicComposite> hColumn)
	{
		return dynamicCompositeTransformer.buildCounterValueFromDynamicComposite(propertyMeta,
				hColumn);
	}

	public <K> List<KeyValue<K, Long>> createCounterKeyValueListForDynamicComposite(
			PropertyMeta<K, Long> propertyMeta, List<HCounterColumn<DynamicComposite>> hColumns)
	{
		return Lists.transform(hColumns,
				dynamicCompositeTransformer.buildCounterKeyValueTransformer(propertyMeta));
	}

	public <K> List<Long> createCounterValueListForDynamicComposite(
			PropertyMeta<K, Long> propertyMeta, List<HCounterColumn<DynamicComposite>> hColumns)
	{
		return Lists.transform(hColumns,
				dynamicCompositeTransformer.buildCounterValueTransformer(propertyMeta));
	}

	public <K> List<K> createCounterKeyListForDynamicComposite(PropertyMeta<K, Long> propertyMeta,
			List<HCounterColumn<DynamicComposite>> hColumns)
	{
		return Lists.transform(hColumns,
				dynamicCompositeTransformer.buildCounterKeyTransformer(propertyMeta));
	}

	private <JOIN_ID, V, ID, K> Map<JOIN_ID, V> loadJoinEntities(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, EntityMeta<JOIN_ID> joinMeta, List<JOIN_ID> joinIds)
	{
		GenericDynamicCompositeDao<JOIN_ID> joinEntityDao = context.findEntityDao(joinMeta
				.getColumnFamilyName());

		Map<JOIN_ID, V> joinEntities = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
				joinIds, joinMeta, joinEntityDao);
		return joinEntities;
	}

	private <V, JOIN_ID, ID> V buildProxy(PersistenceContext<ID> context,
			EntityMeta<JOIN_ID> joinMeta, Map<JOIN_ID, V> joinEntities, Object joinId)
	{
		V joinEntity = joinEntities.get(joinId);
		PersistenceContext<JOIN_ID> joinContext = context.newPersistenceContext(joinMeta,
				joinEntity);
		V proxy = proxifier.buildProxy(joinEntity, joinContext);
		return proxy;
	}
}
