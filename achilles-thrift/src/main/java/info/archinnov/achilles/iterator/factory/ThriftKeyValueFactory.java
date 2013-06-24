package info.archinnov.achilles.iterator.factory;

import static info.archinnov.achilles.logger.ThriftLoggerHelper.format;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftJoinEntityLoader;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValue;

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
 * ThriftKeyValueFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftKeyValueFactory
{
	private static final Logger log = LoggerFactory.getLogger(ThriftKeyValueFactory.class);

	private ThriftJoinEntityLoader joinHelper = new ThriftJoinEntityLoader();
	private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
	private ThriftCompositeTransformer thriftCompositeTransformer = new ThriftCompositeTransformer();

	public <K, V> KeyValue<K, V> createKeyValue(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, V> hColumn)
	{
		log.trace("Build key/value for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return thriftCompositeTransformer.buildKeyValue(context, propertyMeta, hColumn);
	}

	public <K, V> K createKey(PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		log.trace("Build key for property {} of entity class {}", propertyMeta.getPropertyName(),
				propertyMeta.getEntityClassName());
		return thriftCompositeTransformer.buildKey(propertyMeta, hColumn);
	}

	public <K, V> V createValue(ThriftPersistenceContext context, PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, V> hColumn)
	{
		log.trace("Build key value for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return thriftCompositeTransformer.buildValue(context, propertyMeta, hColumn);
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
		return Lists.transform(hColumns,
				thriftCompositeTransformer.buildValueTransformer(propertyMeta));
	}

	public <K, V, W> List<K> createKeyList(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, W>> hColumns)
	{
		log.trace("Build key list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns,
				thriftCompositeTransformer.buildKeyTransformer(propertyMeta));
	}

	public <K, V> List<KeyValue<K, V>> createKeyValueList(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, V>> hColumns)
	{
		log.trace("Build key/value list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns,
				thriftCompositeTransformer.buildKeyValueTransformer(context, propertyMeta));
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
					thriftCompositeTransformer.buildRawValueTransformer());

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
					thriftCompositeTransformer.buildKeyTransformer(propertyMeta));
			List<Object> joinIds = Lists.transform(hColumns,
					thriftCompositeTransformer.buildRawValueTransformer());

			Map<Object, V> joinEntities = loadJoinEntities(context, propertyMeta, joinMeta, joinIds);

			List<Integer> ttls = Lists.transform(hColumns,
					thriftCompositeTransformer.buildTtlTransformer());

			List<Long> timestamps = Lists.transform(hColumns,
					thriftCompositeTransformer.buildTimestampTransformer());

			for (int i = 0; i < keys.size(); i++)
			{
				V proxy = buildProxy(context, joinMeta, joinEntities, joinIds.get(i));
				result.add(new KeyValue<K, V>(keys.get(i), proxy, ttls.get(i), timestamps.get(i)));
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
		return thriftCompositeTransformer.buildCounterKeyValue(context, propertyMeta, hColumn);
	}

	public <K> K createCounterKey(PropertyMeta<K, Counter> propertyMeta,
			HCounterColumn<Composite> hColumn)
	{
		log.trace("Build counter key for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return thriftCompositeTransformer.buildCounterKey(propertyMeta, hColumn);
	}

	public <K> Counter createCounterValue(ThriftPersistenceContext context,
			PropertyMeta<K, Counter> propertyMeta, HCounterColumn<Composite> hColumn)
	{
		log.trace("Build counter value for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return thriftCompositeTransformer.buildCounterValue(context, propertyMeta, hColumn);
	}

	public <K> List<KeyValue<K, Counter>> createCounterKeyValueList(
			ThriftPersistenceContext context, PropertyMeta<K, Counter> propertyMeta,
			List<HCounterColumn<Composite>> hColumns)
	{
		log.trace("Build counter key/value list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns,
				thriftCompositeTransformer.buildCounterKeyValueTransformer(context, propertyMeta));
	}

	public <K> List<Counter> createCounterValueList(ThriftPersistenceContext context,
			PropertyMeta<K, Counter> propertyMeta, List<HCounterColumn<Composite>> hColumns)
	{
		log.trace("Build counter value lsit for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns,
				thriftCompositeTransformer.buildCounterValueTransformer(context, propertyMeta));
	}

	public <K> List<K> createCounterKeyList(PropertyMeta<K, Counter> propertyMeta,
			List<HCounterColumn<Composite>> hColumns)
	{
		log.trace("Build counter key list for property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return Lists.transform(hColumns,
				thriftCompositeTransformer.buildCounterKeyTransformer(propertyMeta));
	}

	private <K, V> Map<Object, V> loadJoinEntities(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta, EntityMeta joinMeta, List<Object> joinIds)
	{
		ThriftGenericEntityDao joinEntityDao = context.findEntityDao(joinMeta.getTableName());

		Map<Object, V> joinEntities = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
				joinIds, joinMeta, joinEntityDao);
		return joinEntities;
	}

	private <V> V buildProxy(ThriftPersistenceContext context, EntityMeta joinMeta,
			Map<Object, V> joinEntities, Object joinId)
	{
		V joinEntity = joinEntities.get(joinId);
		ThriftPersistenceContext joinContext = context.createContextForJoin(joinMeta, joinEntity);
		V proxy = proxifier.buildProxy(joinEntity, joinContext);
		return proxy;
	}
}
