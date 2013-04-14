package info.archinnov.achilles.iterator.factory;

import info.archinnov.achilles.dao.GenericEntityDao;
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

	public <ID, K, V> KeyValue<K, V> createKeyValue(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		return compositeTransformer.buildKeyValue(context, propertyMeta, hColumn);
	}

	public <K, V> K createKey(PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		return compositeTransformer.buildKey(propertyMeta, hColumn);
	}

	public <ID, K, V> V createValue(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		return compositeTransformer.buildValue(context, propertyMeta, hColumn);
	}

	public Integer createTtl(HColumn<Composite, ?> hColumn)
	{
		return hColumn.getTtl();
	}

	public <K, V, W> List<V> createValueList(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, W>> hColumns)
	{
		return Lists.transform(hColumns, compositeTransformer.buildValueTransformer(propertyMeta));
	}

	public <K, V, W> List<K> createKeyList(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, W>> hColumns)
	{
		return Lists.transform(hColumns, compositeTransformer.buildKeyTransformer(propertyMeta));
	}

	public <ID, K, V, W> List<KeyValue<K, V>> createKeyValueList(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, W>> hColumns)
	{
		return Lists.transform(hColumns,
				compositeTransformer.buildKeyValueTransformer(context, propertyMeta));
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, K, V, W> List<V> createJoinValueList(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, W>> hColumns)
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
	public <ID, JOIN_ID, K, V, W> List<KeyValue<K, V>> createJoinKeyValueList(
			PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, W>> hColumns)
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
	public <K> KeyValue<K, Long> createCounterKeyValue(PropertyMeta<K, Long> propertyMeta,
			HCounterColumn<Composite> hColumn)
	{
		return compositeTransformer.buildCounterKeyValue(propertyMeta, hColumn);
	}

	public <K> K createCounterKey(PropertyMeta<K, Long> propertyMeta,
			HCounterColumn<Composite> hColumn)
	{
		return compositeTransformer.buildCounterKey(propertyMeta, hColumn);
	}

	public <K> Long createCounterValue(PropertyMeta<K, Long> propertyMeta,
			HCounterColumn<Composite> hColumn)
	{
		return compositeTransformer.buildCounterValue(propertyMeta, hColumn);
	}

	public <K> List<KeyValue<K, Long>> createCounterKeyValueList(
			PropertyMeta<K, Long> propertyMeta, List<HCounterColumn<Composite>> hColumns)
	{
		return Lists.transform(hColumns,
				compositeTransformer.buildCounterKeyValueTransformer(propertyMeta));
	}

	public <K> List<Long> createCounterValueList(PropertyMeta<K, Long> propertyMeta,
			List<HCounterColumn<Composite>> hColumns)
	{
		return Lists.transform(hColumns,
				compositeTransformer.buildCounterValueTransformer(propertyMeta));
	}

	public <K> List<K> createCounterKeyList(PropertyMeta<K, Long> propertyMeta,
			List<HCounterColumn<Composite>> hColumns)
	{
		return Lists.transform(hColumns,
				compositeTransformer.buildCounterKeyTransformer(propertyMeta));
	}

	private <JOIN_ID, V, ID, K> Map<JOIN_ID, V> loadJoinEntities(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, EntityMeta<JOIN_ID> joinMeta, List<JOIN_ID> joinIds)
	{
		GenericEntityDao<JOIN_ID> joinEntityDao = context.findEntityDao(joinMeta
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
