package info.archinnov.achilles.entity.operations;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.validation.Validator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.commons.lang.StringUtils;

/**
 * EntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityLoader
{
	private EntityMapper mapper = new EntityMapper();
	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();
	private EntityHelper helper = new EntityHelper();

	public <T, ID> T load(Class<T> entityClass, ID key, EntityMeta<ID> entityMeta)
	{
		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(key, "Entity '" + entityClass.getCanonicalName()
				+ "' key should not be null");
		Validator.validateNotNull(entityMeta, "Entity meta for '" + entityClass.getCanonicalName()
				+ "' should not be null");

		T entity = null;
		try
		{

			if (entityMeta.isColumnFamilyDirectMapping())
			{
				entity = entityClass.newInstance();
				helper.setValueToField(entity, entityMeta.getIdMeta().getSetter(), key);
			}
			else
			{
				List<Pair<DynamicComposite, String>> columns = entityMeta.getEntityDao()
						.eagerFetchEntity(key);
				if (columns.size() > 0)
				{
					entity = entityClass.newInstance();
					mapper.setEagerPropertiesToEntity(key, columns, entityMeta, entity);
					helper.setValueToField(entity, entityMeta.getIdMeta().getSetter(), key);

				}
			}

		}
		catch (Exception e)
		{
			throw new RuntimeException("Error when loading entity type '"
					+ entityClass.getCanonicalName() + "' with key '" + key + "'", e);
		}
		return entity;
	}

	public <T, ID> Map<ID, T> loadJoinEntities(Class<T> entityClass, List<ID> keys,
			EntityMeta<ID> entityMeta)
	{
		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotEmpty(keys, "List of join primary keys '" + keys
				+ "' should not be empty");
		Validator.validateNotNull(entityMeta, "Entity meta for '" + entityClass.getCanonicalName()
				+ "' should not be null");

		Map<ID, T> entitiesByKey = new HashMap<ID, T>();
		Map<ID, List<Pair<DynamicComposite, String>>> rows = entityMeta.getEntityDao()
				.eagerFetchEntities(keys);

		for (Entry<ID, List<Pair<DynamicComposite, String>>> entry : rows.entrySet())
		{
			T entity;
			try
			{
				entity = entityClass.newInstance();

				ID key = entry.getKey();
				List<Pair<DynamicComposite, String>> columns = entry.getValue();
				if (columns.size() > 0)
				{
					mapper.setEagerPropertiesToEntity(key, columns, entityMeta, entity);
					helper.setValueToField(entity, entityMeta.getIdMeta().getSetter(), key);
					// TODO Refactor
					// helper.buildProxy(entity, entityMeta)
					entitiesByKey.put(key, entity);
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException("Error when instantiating class '"
						+ entityClass.getCanonicalName() + "' ", e);
			}
		}
		return entitiesByKey;
	}

	protected <ID, V> Long loadVersionSerialUID(ID key, GenericDynamicCompositeDao<ID> dao)
	{
		DynamicComposite composite = new DynamicComposite();
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

	protected <ID, V> V loadSimpleProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> propertyMeta)
	{
		DynamicComposite composite = keyFactory.createBaseForQuery(propertyMeta, EQUAL);
		return propertyMeta.getValueFromString(dao.getValue(key, composite));
	}

	protected <ID, V> List<V> loadListProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> listPropertyMeta)
	{
		DynamicComposite start = keyFactory.createBaseForQuery(listPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(listPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		List<V> list = listPropertyMeta.newListInstance();
		for (Pair<DynamicComposite, String> pair : columns)
		{
			list.add(listPropertyMeta.getValueFromString(pair.right));
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	protected <ID, JOIN_ID, V> List<V> loadJoinListProperty(ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, V> listPropertyMeta)
	{
		DynamicComposite start = keyFactory.createBaseForQuery(listPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(listPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		List<JOIN_ID> joinIds = new ArrayList<JOIN_ID>();

		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) listPropertyMeta.getJoinProperties()
				.getEntityMeta();
		PropertyMeta<Void, ?> joinIdMeta = joinMeta.getIdMeta();

		for (Pair<DynamicComposite, String> pair : columns)
		{
			joinIds.add((JOIN_ID) joinIdMeta.getValueFromString(pair.right));
		}

		List<V> joinEntities = new ArrayList<V>();
		if (joinIds.size() > 0)
		{
			Map<JOIN_ID, V> entitiesMap = this.loadJoinEntities(listPropertyMeta.getValueClass(),
					(List<JOIN_ID>) joinIds, joinMeta);

			for (JOIN_ID joinId : joinIds)
			{
				joinEntities.add(entitiesMap.get(joinId));
			}
		}

		return joinEntities;
	}

	protected <ID, V> Set<V> loadSetProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> setPropertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(setPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(setPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		Set<V> set = setPropertyMeta.newSetInstance();
		for (Pair<DynamicComposite, String> pair : columns)
		{
			set.add(setPropertyMeta.getValueFromString(pair.right));
		}
		return set;
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	protected <ID, V> Set<V> loadJoinSetProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> setPropertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(setPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(setPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);

		List<Object> joinIds = new ArrayList<Object>();

		EntityMeta<?> joinMeta = setPropertyMeta.getJoinProperties().getEntityMeta();
		PropertyMeta<Void, ?> joinIdMeta = joinMeta.getIdMeta();

		for (Pair<DynamicComposite, String> pair : columns)
		{
			joinIds.add(joinIdMeta.getValueFromString(pair.right));
		}

		Set<V> joinEntities = new HashSet<V>();
		if (joinIds.size() > 0)
		{
			Map<Object, V> entitiesMap = this.loadJoinEntities(setPropertyMeta.getValueClass(),
					(List) joinIds, joinMeta);

			for (Object joinId : joinIds)
			{
				joinEntities.add(entitiesMap.get(joinId));
			}
		}

		return joinEntities;
	}

	protected <ID, K, V> Map<K, V> loadMapProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<K, V> mapPropertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(mapPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(mapPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		Map<K, V> map = mapPropertyMeta.newMapInstance();

		Class<K> keyClass = mapPropertyMeta.getKeyClass();
		for (Pair<DynamicComposite, String> pair : columns)
		{
			KeyValue<K, V> holder = mapPropertyMeta.getKeyValueFromString(pair.right);

			map.put(keyClass.cast(holder.getKey()),
					mapPropertyMeta.getValueFromString(holder.getValue()));
		}
		return map;
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	protected <ID, K, V> Map<K, V> loadJoinMapProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<K, V> mapPropertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(mapPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(mapPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);

		EntityMeta<?> joinMeta = mapPropertyMeta.getJoinProperties().getEntityMeta();
		PropertyMeta<Void, ?> joinIdMeta = joinMeta.getIdMeta();

		Map<K, V> map = mapPropertyMeta.newMapInstance();
		Map<K, Object> partialMap = new HashMap<K, Object>();

		Class<K> keyClass = mapPropertyMeta.getKeyClass();

		List<Object> joinIds = new ArrayList<Object>();

		for (Pair<DynamicComposite, String> pair : columns)
		{
			KeyValue<K, V> holder = mapPropertyMeta.getKeyValueFromString(pair.right);

			Object joinId = joinIdMeta.getValueFromString(holder.getValue());
			partialMap.put(keyClass.cast(holder.getKey()), joinId);
			joinIds.add(joinId);
		}

		if (joinIds.size() > 0)
		{
			Map<Object, V> entitiesMap = this.loadJoinEntities(mapPropertyMeta.getValueClass(),
					(List) joinIds, joinMeta);

			for (Entry<K, Object> entry : partialMap.entrySet())
			{
				map.put(entry.getKey(), entitiesMap.get(entry.getValue()));
			}
		}

		return map;
	}

	public <ID, V> void loadPropertyIntoObject(Object realObject, ID key,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, V> propertyMeta)
	{
		Object value = null;
		switch (propertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				value = this.loadSimpleProperty(key, dao, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				value = this.loadListProperty(key, dao, propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				value = this.loadSetProperty(key, dao, propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				value = this.loadMapProperty(key, dao, propertyMeta);
				break;
			case JOIN_SIMPLE:
				value = this.loadJoinColumn(key, dao, propertyMeta);
				break;
			case JOIN_LIST:
				value = this.loadJoinListProperty(key, dao, propertyMeta);
				break;
			case JOIN_SET:
				value = this.loadJoinSetProperty(key, dao, propertyMeta);
				break;
			case JOIN_MAP:
				value = this.loadJoinMapProperty(key, dao, propertyMeta);
				break;
			default:
				return;
		}
		helper.setValueToField(realObject, propertyMeta.getSetter(), value);
	}

	public <JOIN_ID, V> V loadJoinEntity(Class<V> entityClass, JOIN_ID joinId,
			EntityMeta<JOIN_ID> joinEntityMeta)
	{

		return this.load(entityClass, joinId, joinEntityMeta);
		// if (joinEntity != null)
		// {
		// return helper.buildProxy(joinEntity, joinEntityMeta);
		// }
		// else
		// {
		// return null;
		// }

	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	protected <ID, V> V loadJoinColumn(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> joinPropertyMeta)
	{
		EntityMeta joinEntityMeta = joinPropertyMeta.getJoinProperties().getEntityMeta();
		DynamicComposite composite = keyFactory.createBaseForQuery(joinPropertyMeta, EQUAL);

		Object joinId = dao.getValue(key, composite);

		if (joinId != null)
		{
			joinId = joinEntityMeta.getIdMeta().getValueFromString(joinId);
			return (V) this
					.loadJoinEntity(joinPropertyMeta.getValueClass(), joinId, joinEntityMeta);

		}
		else
		{
			return null;
		}

	}
}
