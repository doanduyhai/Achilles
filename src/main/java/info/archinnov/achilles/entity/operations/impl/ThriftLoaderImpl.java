package info.archinnov.achilles.entity.operations.impl;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.lang.StringUtils;

/**
 * ThriftLoaderImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftLoaderImpl
{
	private EntityMapper mapper = new EntityMapper();
	private EntityIntrospector introspector = new EntityIntrospector();
	private CompositeFactory compositeFactory = new CompositeFactory();

	@SuppressWarnings("unchecked")
	public <T, ID> T load(PersistenceContext<ID> context) throws Exception
	{
		Class<T> entityClass = (Class<T>) context.getEntityClass();
		EntityMeta<ID> entityMeta = context.getEntityMeta();
		ID primaryKey = context.getPrimaryKey();

		List<Pair<Composite, String>> columns = context.getEntityDao().eagerFetchEntity(primaryKey);
		T entity = null;
		if (columns.size() > 0)
		{
			entity = entityClass.newInstance();
			mapper.setEagerPropertiesToEntity(primaryKey, columns, entityMeta, entity);
			introspector.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);
		}

		return entity;
	}

	public <ID, V> Long loadVersionSerialUID(ID key, GenericEntityDao<ID> dao)
	{
		Composite composite = new Composite();
		composite.addComponent(0, PropertyType.SERIAL_VERSION_UID.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, PropertyType.SERIAL_VERSION_UID.name(), ComponentEquality.EQUAL);
		composite.addComponent(2, 0, ComponentEquality.EQUAL);

		String serialVersionUIDString = dao.getValue(key, composite);
		if (StringUtils.isNotBlank(serialVersionUIDString))
		{
			return Long.parseLong(serialVersionUIDString);
		}
		else
		{
			return null;
		}
	}

	public <ID, V> V loadSimpleProperty(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		Composite composite = compositeFactory.createBaseForGet(propertyMeta);
		return propertyMeta.getValueFromString(context.getEntityDao().getValue(
				context.getPrimaryKey(), composite));
	}

	@SuppressWarnings("unchecked")
	public <ID> Long loadSimpleCounterProperty(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta)
	{
		Composite keyComp = compositeFactory.createKeyForCounter(propertyMeta.fqcn(),
				context.getPrimaryKey(), (PropertyMeta<Void, ID>) propertyMeta.counterIdMeta());
		Composite comp = compositeFactory.createBaseForCounterGet(propertyMeta);

		return loadCounterWithConsistencyLevel(context, propertyMeta, keyComp, comp);
	}

	private <ID> Long loadCounterWithConsistencyLevel(PersistenceContext<ID> context,
			PropertyMeta<?, ?> propertyMeta, Composite keyComp, Composite comp)
	{
		boolean resetConsistencyLevel = false;
		AchillesConfigurableConsistencyLevelPolicy policy = context.getPolicy();
		if (policy.getCurrentReadLevel() == null)
		{
			policy.setCurrentReadLevel(propertyMeta.getReadConsistencyLevel());
			resetConsistencyLevel = true;
		}
		Long counter;
		try
		{
			counter = context.getCounterDao().getCounterValue(keyComp, comp);
		}
		finally
		{
			if (resetConsistencyLevel)
			{
				policy.removeCurrentReadLevel();
			}
		}
		return counter;
	}

	public <ID, V> List<V> loadListProperty(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		List<Pair<Composite, String>> columns = fetchColumns(context, propertyMeta);
		List<V> list = null;
		if (columns.size() > 0)
		{
			list = propertyMeta.newListInstance();
			for (Pair<Composite, String> pair : columns)
			{
				list.add(propertyMeta.getValueFromString(pair.right));
			}
		}
		return list;
	}

	public <ID, V> Set<V> loadSetProperty(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{

		List<Pair<Composite, String>> columns = fetchColumns(context, propertyMeta);
		Set<V> set = null;
		if (columns.size() > 0)
		{
			set = propertyMeta.newSetInstance();
			for (Pair<Composite, String> pair : columns)
			{
				set.add(propertyMeta.getValueFromString(pair.right));
			}
		}
		return set;
	}

	public <ID, K, V> Map<K, V> loadMapProperty(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta)
	{
		List<Pair<Composite, String>> columns = fetchColumns(context, propertyMeta);
		Class<K> keyClass = propertyMeta.getKeyClass();
		Map<K, V> map = null;
		if (columns.size() > 0)
		{
			map = propertyMeta.newMapInstance();
			for (Pair<Composite, String> pair : columns)
			{
				KeyValue<K, V> holder = propertyMeta.getKeyValueFromString(pair.right);

				map.put(keyClass.cast(holder.getKey()), propertyMeta.castValue(holder.getValue()));
			}
		}
		return map;
	}

	private <ID, V> List<Pair<Composite, String>> fetchColumns(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta)
	{
		Composite start = compositeFactory.createBaseForQuery(propertyMeta, EQUAL);
		Composite end = compositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		List<Pair<Composite, String>> columns = context.getEntityDao().findColumnsRange(
				context.getPrimaryKey(), start, end, false, Integer.MAX_VALUE);
		return columns;
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, V> V loadJoinSimple(PersistenceContext<ID> context,
			PropertyMeta<?, V> propertyMeta, EntityLoader loader)
	{
		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
		PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
				.joinIdMeta();

		Composite composite = compositeFactory.createBaseForGet(propertyMeta);

		String stringJoinId = context.getEntityDao().getValue(context.getPrimaryKey(), composite);

		if (stringJoinId != null)
		{
			JOIN_ID joinId = joinIdMeta.getValueFromString(stringJoinId);
			PersistenceContext<JOIN_ID> joinContext = context.newPersistenceContext(
					propertyMeta.getValueClass(), joinMeta, joinId);
			return loader.load(joinContext);

		}
		else
		{
			return null;
		}
	}
}
