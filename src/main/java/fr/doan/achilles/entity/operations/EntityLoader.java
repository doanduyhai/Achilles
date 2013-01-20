package fr.doan.achilles.entity.operations;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.cassandra.utils.Pair;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericDynamicCompositeDao;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.EntityMapper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.proxy.builder.EntityProxyBuilder;
import fr.doan.achilles.validation.Validator;

/**
 * EntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityLoader
{
	private EntityProxyBuilder interceptorBuilder = new EntityProxyBuilder();
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

			if (entityMeta.isWideRow())
			{
				entity = entityClass.newInstance();
				helper.setValueToField(entity, entityMeta.getIdMeta().getSetter(), key);
			}
			else
			{
				List<Pair<DynamicComposite, Object>> columns = entityMeta.getEntityDao()
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
		Map<ID, List<Pair<DynamicComposite, Object>>> rows = entityMeta.getEntityDao()
				.eagerFetchEntities(keys);

		for (Entry<ID, List<Pair<DynamicComposite, Object>>> entry : rows.entrySet())
		{
			T entity;
			try
			{
				entity = entityClass.newInstance();

				ID key = entry.getKey();
				List<Pair<DynamicComposite, Object>> columns = entry.getValue();
				if (columns.size() > 0)
				{
					mapper.setEagerPropertiesToEntity(key, columns, entityMeta, entity);
					helper.setValueToField(entity, entityMeta.getIdMeta().getSetter(), key);

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

	protected <ID, V> V loadSimpleProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> propertyMeta)
	{
		DynamicComposite composite = keyFactory.createBaseForQuery(propertyMeta, EQUAL);
		Object value = dao.getValue(key, composite);

		return propertyMeta.getValue(value);
	}

	protected <ID, V> List<V> loadListProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> listPropertyMeta)
	{
		DynamicComposite start = keyFactory.createBaseForQuery(listPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(listPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		List<V> list = listPropertyMeta.newListInstance();
		for (Pair<DynamicComposite, Object> pair : columns)
		{
			list.add(listPropertyMeta.getValue(pair.right));
		}
		return list;
	}

	protected <ID, V> Set<V> loadSetProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<?, V> setPropertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(setPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(setPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		Set<V> set = setPropertyMeta.newSetInstance();
		for (Pair<DynamicComposite, Object> pair : columns)
		{
			set.add(setPropertyMeta.getValue(pair.right));
		}
		return set;
	}

	@SuppressWarnings("unchecked")
	protected <ID, K, V> Map<K, V> loadMapProperty(ID key, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<K, V> mapPropertyMeta)
	{

		DynamicComposite start = keyFactory.createBaseForQuery(mapPropertyMeta, EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(mapPropertyMeta, GREATER_THAN_EQUAL);
		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(key, start, end, false,
				Integer.MAX_VALUE);
		Map<K, V> map = mapPropertyMeta.newMapInstance();

		Class<K> keyClass = mapPropertyMeta.getKeyClass();
		for (Pair<DynamicComposite, Object> pair : columns)
		{
			KeyValue<K, V> holder = (KeyValue<K, V>) pair.right;

			map.put(keyClass.cast(holder.getKey()), mapPropertyMeta.getValue(holder.getValue()));
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
			default:
				return;
		}
		helper.setValueToField(realObject, propertyMeta.getSetter(), value);
	}

	public <JOIN_ID, V> V loadJoinEntity(Class<V> entityClass, JOIN_ID joinId,
			EntityMeta<JOIN_ID> joinEntityMeta)
	{
		V joinEntity = this.load(entityClass, joinId, joinEntityMeta);
		if (joinEntity != null)
		{
			return this.interceptorBuilder.build(joinEntity, joinEntityMeta);
		}
		else
		{
			return null;
		}

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
			return (V) this
					.loadJoinEntity(joinPropertyMeta.getValueClass(), joinId, joinEntityMeta);
		}
		else
		{
			return null;
		}

	}
}
