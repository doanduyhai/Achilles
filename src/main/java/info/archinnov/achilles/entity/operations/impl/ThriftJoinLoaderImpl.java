package info.archinnov.achilles.entity.operations.impl;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.ThriftJoinEntityHelper;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftJoinLoaderImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftJoinLoaderImpl
{
	private static final Logger log = LoggerFactory.getLogger(ThriftJoinLoaderImpl.class);

	private ThriftCompositeFactory thriftCompositeFactory = new ThriftCompositeFactory();
	private ThriftJoinEntityHelper joinHelper = new ThriftJoinEntityHelper();

	public <V> List<V> loadJoinListProperty(ThriftPersistenceContext context,
			PropertyMeta<?, V> propertyMeta)
	{

		EntityMeta joinMeta = propertyMeta.joinMeta();
		List<Object> joinIds = fetchColumns(context, propertyMeta);
		log.trace("Loading join entities of class {} having primary keys {}", propertyMeta
				.getValueClass()
				.getCanonicalName(), joinIds);

		ThriftGenericEntityDao joinEntityDao = context.findEntityDao(joinMeta.getTableName());
		List<V> joinEntities = new ArrayList<V>();
		fillCollectionWithJoinEntities(propertyMeta, joinMeta, joinIds, joinEntityDao, joinEntities);

		return joinEntities;
	}

	public <V> Set<V> loadJoinSetProperty(ThriftPersistenceContext context,
			PropertyMeta<?, V> propertyMeta)
	{
		EntityMeta joinMeta = propertyMeta.joinMeta();
		List<Object> joinIds = fetchColumns(context, propertyMeta);
		ThriftGenericEntityDao joinEntityDao = context.findEntityDao(joinMeta.getTableName());
		Set<V> joinEntities = new HashSet<V>();
		fillCollectionWithJoinEntities(propertyMeta, joinMeta, joinIds, joinEntityDao, joinEntities);

		return joinEntities;
	}

	public <K, V> Map<K, V> loadJoinMapProperty(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta)
	{

		EntityMeta joinMeta = propertyMeta.joinMeta();
		ThriftGenericEntityDao joinEntityDao = context.findEntityDao(joinMeta.getTableName());

		Composite start = thriftCompositeFactory.createBaseForQuery(propertyMeta, EQUAL);
		Composite end = thriftCompositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		List<Pair<Composite, String>> columns = context.getEntityDao().findColumnsRange(
				context.getPrimaryKey(), start, end, false, Integer.MAX_VALUE);

		PropertyMeta<?, ?> joinIdMeta = propertyMeta.joinIdMeta();

		Map<K, V> map = new HashMap<K, V>();
		Map<K, Object> partialMap = new HashMap<K, Object>();

		Class<K> keyClass = propertyMeta.getKeyClass();

		List<Object> joinIds = new ArrayList<Object>();

		for (Pair<Composite, String> pair : columns)
		{
			KeyValue<K, V> holder = propertyMeta.getKeyValueFromString(pair.right);

			Object joinId = joinIdMeta.getValueFromString(holder.getValue());
			partialMap.put(keyClass.cast(holder.getKey()), joinId);
			joinIds.add(joinId);
		}

		if (joinIds.size() > 0)
		{
			log.trace("Loading join entities of class {} having primary keys {}", propertyMeta
					.getValueClass()
					.getCanonicalName(), joinIds);

			Map<Object, V> entitiesMap = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
					joinIds, joinMeta, joinEntityDao);

			for (Entry<K, Object> entry : partialMap.entrySet())
			{
				map.put(entry.getKey(), entitiesMap.get(entry.getValue()));
			}
		}

		return map;
	}

	private <V> List<Object> fetchColumns(ThriftPersistenceContext context,
			PropertyMeta<?, V> propertyMeta)
	{

		log.trace("Fetching join keys for property {} of class {} ",
				propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName());

		Composite start = thriftCompositeFactory.createBaseForQuery(propertyMeta, EQUAL);
		Composite end = thriftCompositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		List<Pair<Composite, String>> columns = context.getEntityDao().findColumnsRange(
				context.getPrimaryKey(), start, end, false, Integer.MAX_VALUE);
		List<Object> joinIds = new ArrayList<Object>();

		PropertyMeta<?, ?> joinIdMeta = propertyMeta.joinIdMeta();

		for (Pair<Composite, String> pair : columns)
		{
			joinIds.add(joinIdMeta.getValueFromString(pair.right));
		}
		return joinIds;
	}

	private <V> void fillCollectionWithJoinEntities(PropertyMeta<?, V> propertyMeta,
			EntityMeta joinMeta, List<Object> joinIds, ThriftGenericEntityDao joinEntityDao,
			Collection<V> joinEntities)
	{
		if (joinIds.size() > 0)
		{
			Map<Object, V> entitiesMap = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
					joinIds, joinMeta, joinEntityDao);

			for (Object joinId : joinIds)
			{
				joinEntities.add(entitiesMap.get(joinId));
			}
		}
	}
}
