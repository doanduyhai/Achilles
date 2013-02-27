package info.archinnov.achilles.entity.operations;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.JoinEntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * JoinEntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinEntityLoader
{
	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();
	private JoinEntityHelper joinHelper = new JoinEntityHelper();

	@SuppressWarnings("unchecked")
	protected <ID, JOIN_ID, V> List<V> loadJoinListProperty(ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, V> propertyMeta)
	{
		DynamicComposite start = keyFactory.createBaseForQuery(propertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		List<JOIN_ID> joinIds = new ArrayList<JOIN_ID>();

		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		PropertyMeta<Void, ?> joinIdMeta = propertyMeta.joinIdMeta();

		for (Pair<DynamicComposite, String> pair : columns)
		{
			joinIds.add((JOIN_ID) joinIdMeta.getValueFromString(pair.right));
		}

		List<V> joinEntities = new ArrayList<V>();
		if (joinIds.size() > 0)
		{
			Map<JOIN_ID, V> entitiesMap = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
					(List<JOIN_ID>) joinIds, joinMeta);

			for (JOIN_ID joinId : joinIds)
			{
				joinEntities.add(entitiesMap.get(joinId));
			}
		}

		return joinEntities;
	}

	@SuppressWarnings("unchecked")
	protected <ID, JOIN_ID, V> Set<V> loadJoinSetProperty(ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, V> propertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(propertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);

		List<JOIN_ID> joinIds = new ArrayList<JOIN_ID>();

		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
				.joinIdMeta();

		for (Pair<DynamicComposite, String> pair : columns)
		{
			joinIds.add(joinIdMeta.getValueFromString(pair.right));
		}

		Set<V> joinEntities = new HashSet<V>();
		if (joinIds.size() > 0)
		{
			Map<JOIN_ID, V> entitiesMap = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
					(List<JOIN_ID>) joinIds, joinMeta);

			for (JOIN_ID joinId : joinIds)
			{
				joinEntities.add(entitiesMap.get(joinId));
			}
		}

		return joinEntities;
	}

	@SuppressWarnings("unchecked")
	protected <ID, JOIN_ID, K, V> Map<K, V> loadJoinMapProperty(ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<K, V> propertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(propertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);

		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
				.joinIdMeta();

		Map<K, V> map = propertyMeta.newMapInstance();
		Map<K, JOIN_ID> partialMap = new HashMap<K, JOIN_ID>();

		Class<K> keyClass = propertyMeta.getKeyClass();

		List<JOIN_ID> joinIds = new ArrayList<JOIN_ID>();

		for (Pair<DynamicComposite, String> pair : columns)
		{
			KeyValue<K, V> holder = propertyMeta.getKeyValueFromString(pair.right);

			JOIN_ID joinId = joinIdMeta.getValueFromString(holder.getValue());
			partialMap.put(keyClass.cast(holder.getKey()), joinId);
			joinIds.add(joinId);
		}

		if (joinIds.size() > 0)
		{
			Map<JOIN_ID, V> entitiesMap = joinHelper.loadJoinEntities(propertyMeta.getValueClass(),
					(List<JOIN_ID>) joinIds, joinMeta);

			for (Entry<K, JOIN_ID> entry : partialMap.entrySet())
			{
				map.put(entry.getKey(), entitiesMap.get(entry.getValue()));
			}
		}

		return map;
	}

}
