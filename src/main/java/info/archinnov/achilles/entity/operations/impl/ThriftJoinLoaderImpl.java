package info.archinnov.achilles.entity.operations.impl;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.ThriftJoinEntityHelper;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.Pair;

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
 * JoinEntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftJoinLoaderImpl
{
	private static final Logger log = LoggerFactory.getLogger(ThriftJoinLoaderImpl.class);

	private CompositeFactory compositeFactory = new CompositeFactory();
	private ThriftJoinEntityHelper joinHelper = new ThriftJoinEntityHelper();

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, V> List<V> loadJoinListProperty(ThriftPersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{

		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		List<JOIN_ID> joinIds = fetchColumns(context, propertyMeta);
		log.trace("Loading join entities of class {} having primary keys {}", propertyMeta
				.getValueClass().getCanonicalName(), joinIds);

		ThriftGenericEntityDao<JOIN_ID> joinEntityDao = (ThriftGenericEntityDao<JOIN_ID>) context
				.findEntityDao(joinMeta.getColumnFamilyName());
		List<V> joinEntities = new ArrayList<V>();
		fillCollectionWithJoinEntities(propertyMeta, joinMeta, joinIds, joinEntityDao, joinEntities);

		return joinEntities;
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, V> Set<V> loadJoinSetProperty(ThriftPersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		List<JOIN_ID> joinIds = fetchColumns(context, propertyMeta);
		ThriftGenericEntityDao<JOIN_ID> joinEntityDao = (ThriftGenericEntityDao<JOIN_ID>) context
				.findEntityDao(joinMeta.getColumnFamilyName());
		Set<V> joinEntities = new HashSet<V>();
		fillCollectionWithJoinEntities(propertyMeta, joinMeta, joinIds, joinEntityDao, joinEntities);

		return joinEntities;
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, K, V> Map<K, V> loadJoinMapProperty(ThriftPersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta)
	{

		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		ThriftGenericEntityDao<JOIN_ID> joinEntityDao = (ThriftGenericEntityDao<JOIN_ID>) context
				.findEntityDao(joinMeta.getColumnFamilyName());

		Composite start = compositeFactory.createBaseForQuery(propertyMeta, EQUAL);
		Composite end = compositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		List<Pair<Composite, String>> columns = context.getEntityDao().findColumnsRange(
				context.getPrimaryKey(), start, end, false, Integer.MAX_VALUE);

		PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
				.joinIdMeta();

		Map<K, V> map = propertyMeta.newMapInstance();
		Map<K, JOIN_ID> partialMap = new HashMap<K, JOIN_ID>();

		Class<K> keyClass = propertyMeta.getKeyClass();

		List<JOIN_ID> joinIds = new ArrayList<JOIN_ID>();

		for (Pair<Composite, String> pair : columns)
		{
			KeyValue<K, V> holder = propertyMeta.getKeyValueFromString(pair.right);

			JOIN_ID joinId = joinIdMeta.getValueFromString(holder.getValue());
			partialMap.put(keyClass.cast(holder.getKey()), joinId);
			joinIds.add(joinId);
		}

		if (joinIds.size() > 0)
		{
			log.trace("Loading join entities of class {} having primary keys {}", propertyMeta
					.getValueClass().getCanonicalName(), joinIds);

			Map<JOIN_ID, V> entitiesMap = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
					(List<JOIN_ID>) joinIds, joinMeta, joinEntityDao);

			for (Entry<K, JOIN_ID> entry : partialMap.entrySet())
			{
				map.put(entry.getKey(), entitiesMap.get(entry.getValue()));
			}
		}

		return map;
	}

	@SuppressWarnings("unchecked")
	private <JOIN_ID, ID, V> List<JOIN_ID> fetchColumns(ThriftPersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{

		log.trace("Fetching join keys for property {} of class {} ",
				propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName());

		Composite start = compositeFactory.createBaseForQuery(propertyMeta, EQUAL);
		Composite end = compositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		List<Pair<Composite, String>> columns = context.getEntityDao().findColumnsRange(
				context.getPrimaryKey(), start, end, false, Integer.MAX_VALUE);
		List<JOIN_ID> joinIds = new ArrayList<JOIN_ID>();

		PropertyMeta<Void, ?> joinIdMeta = propertyMeta.joinIdMeta();

		for (Pair<Composite, String> pair : columns)
		{
			joinIds.add((JOIN_ID) joinIdMeta.getValueFromString(pair.right));
		}
		return joinIds;
	}

	private <V, JOIN_ID> void fillCollectionWithJoinEntities(PropertyMeta<?, V> propertyMeta,
			EntityMeta<JOIN_ID> joinMeta, List<JOIN_ID> joinIds,
			ThriftGenericEntityDao<JOIN_ID> joinEntityDao, Collection<V> joinEntities)
	{
		if (joinIds.size() > 0)
		{
			Map<JOIN_ID, V> entitiesMap = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
					(List<JOIN_ID>) joinIds, joinMeta, joinEntityDao);

			for (JOIN_ID joinId : joinIds)
			{
				joinEntities.add(entitiesMap.get(joinId));
			}
		}
	}
}
