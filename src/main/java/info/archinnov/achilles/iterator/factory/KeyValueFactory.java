package info.archinnov.achilles.iterator.factory;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.format;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.ThriftJoinEntityHelper;
import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.AchillesEntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * KeyValueFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueFactory
{
	private static final Logger log = LoggerFactory.getLogger(KeyValueFactory.class);

	private ThriftJoinEntityHelper joinHelper = new ThriftJoinEntityHelper();
	private AchillesEntityProxifier proxifier = new ThriftEntityProxifier();
	private CompositeTransformer compositeTransformer = new CompositeTransformer();

	public <K, V> KeyValue<K, V> createKeyValue(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, V> hColumn)
	{
		log.trace("Build key/value for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return compositeTransformer.buildKeyValue(context, propertyMeta, hColumn);
	}

	public <K, V> K createKey(PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		log.trace("Build key for property {} of entity class {}", propertyMeta.getPropertyName(),
				propertyMeta.getEntityClassName());
		return compositeTransformer.buildKey(propertyMeta, hColumn);
	}

	public <K, V> V createValue(ThriftPersistenceContext context, PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, V> hColumn)
	{
		log.trace("Build key value for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return compositeTransformer.buildValue(context, propertyMeta, hColumn);
	}

	public Integer createTtl(HColumn<Composite, ?> hColumn)
	{
		log.debug("Build ttl from Hcolumn {}", format(hColumn.getName()));
		return hColumn.getTtl();
	}

	public <K, V, W> List<V> createValueList(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, W>> hColumns)
	{
		log.trace("Build value list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns, compositeTransformer.buildValueTransformer(propertyMeta));
	}

	public <K, V, W> List<K> createKeyList(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, W>> hColumns)
	{
		log.trace("Build key list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns, compositeTransformer.buildKeyTransformer(propertyMeta));
	}

	public <K, V> List<KeyValue<K, V>> createKeyValueList(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, V>> hColumns)
	{
		log.trace("Build key/value list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns,
				compositeTransformer.buildKeyValueTransformer(context, propertyMeta));
	}

	public <K, V, W> List<V> createJoinValueList(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, W>> hColumns)
	{
		log.trace("Build join value list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		List<V> result = new ArrayList<V>();

		if (CollectionUtils.isNotEmpty(hColumns))
		{

			EntityMeta joinMeta = propertyMeta.joinMeta();

			List<Object> joinIds = Lists.transform(hColumns,
					compositeTransformer.buildRawValueTransformer());

			Map<Object, V> joinEntities = loadJoinEntities(context, propertyMeta, joinMeta, joinIds);

			for (Object joinId : joinIds)
			{
				V proxy = buildProxy(context, joinMeta, joinEntities, joinId);
				result.add(proxy);
			}
		}

		return result;
	}

	public <K, V, W> List<KeyValue<K, V>> createJoinKeyValueList(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, W>> hColumns)
	{
		log.trace("Build join key/value list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();

		if (CollectionUtils.isNotEmpty(hColumns))
		{

			EntityMeta joinMeta = propertyMeta.joinMeta();
			List<K> keys = Lists.transform(hColumns,
					compositeTransformer.buildKeyTransformer(propertyMeta));
			List<Object> joinIds = Lists.transform(hColumns,
					compositeTransformer.buildRawValueTransformer());

			Map<Object, V> joinEntities = loadJoinEntities(context, propertyMeta, joinMeta, joinIds);

			List<Integer> ttls = Lists.transform(hColumns,
					compositeTransformer.buildTtlTransformer());

			for (int i = 0; i < keys.size(); i++)
			{
				V proxy = buildProxy(context, joinMeta, joinEntities, joinIds.get(i));
				result.add(new KeyValue<K, V>(keys.get(i), proxy, ttls.get(i)));
			}
		}
		return result;
	}

	// Counter
	public <K> KeyValue<K, Counter> createCounterKeyValue(ThriftPersistenceContext context,
			PropertyMeta<K, Counter> propertyMeta, HCounterColumn<Composite> hColumn)
	{
		log.trace("Build counter key/value for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return compositeTransformer.buildCounterKeyValue(context, propertyMeta, hColumn);
	}

	public <K> K createCounterKey(PropertyMeta<K, Counter> propertyMeta,
			HCounterColumn<Composite> hColumn)
	{
		log.trace("Build counter key for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return compositeTransformer.buildCounterKey(propertyMeta, hColumn);
	}

	public <K> Counter createCounterValue(ThriftPersistenceContext context,
			PropertyMeta<K, Counter> propertyMeta, HCounterColumn<Composite> hColumn)
	{
		log.trace("Build counter value for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return compositeTransformer.buildCounterValue(context, propertyMeta, hColumn);
	}

	public <K> List<KeyValue<K, Counter>> createCounterKeyValueList(
			ThriftPersistenceContext context, PropertyMeta<K, Counter> propertyMeta,
			List<HCounterColumn<Composite>> hColumns)
	{
		log.trace("Build counter key/value list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns,
				compositeTransformer.buildCounterKeyValueTransformer(context, propertyMeta));
	}

	public <K> List<Counter> createCounterValueList(ThriftPersistenceContext context,
			PropertyMeta<K, Counter> propertyMeta, List<HCounterColumn<Composite>> hColumns)
	{
		log.trace("Build counter value lsit for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns,
				compositeTransformer.buildCounterValueTransformer(context, propertyMeta));
	}

	public <K> List<K> createCounterKeyList(PropertyMeta<K, Counter> propertyMeta,
			List<HCounterColumn<Composite>> hColumns)
	{
		log.trace("Build counter key list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns,
				compositeTransformer.buildCounterKeyTransformer(propertyMeta));
	}

	private <K, V> Map<Object, V> loadJoinEntities(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta, EntityMeta joinMeta, List<Object> joinIds)
	{
		ThriftGenericEntityDao joinEntityDao = context
				.findEntityDao(joinMeta.getTableName());

		Map<Object, V> joinEntities = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
				joinIds, joinMeta, joinEntityDao);
		return joinEntities;
	}

	private <V> V buildProxy(ThriftPersistenceContext context, EntityMeta joinMeta,
			Map<Object, V> joinEntities, Object joinId)
	{
		V joinEntity = joinEntities.get(joinId);
		AchillesPersistenceContext joinContext = context
				.newPersistenceContext(joinMeta, joinEntity);
		V proxy = proxifier.buildProxy(joinEntity, joinContext);
		return proxy;
	}
}
