package info.archinnov.achilles.entity;

import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.exception.AchillesException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityMapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMapper
{

	private static final Logger log = LoggerFactory.getLogger(EntityMapper.class);

	private EntityIntrospector introspector = new EntityIntrospector();

	@SuppressWarnings("unchecked")
	public <T, ID, K, V> void setEagerPropertiesToEntity(ID key,
			List<Pair<Composite, String>> columns, EntityMeta<ID> entityMeta, T entity)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Set eager properties to entity {} ", entityMeta.getClassName());
		}

		Map<String, List<V>> listProperties = new HashMap<String, List<V>>();
		Map<String, Set<V>> setProperties = new HashMap<String, Set<V>>();
		Map<String, Map<K, V>> mapProperties = new HashMap<String, Map<K, V>>();

		setIdToEntity(key, entityMeta.getIdMeta(), entity);

		Map<String, PropertyMeta<?, ?>> propertyMetas = entityMeta.getPropertyMetas();

		for (Pair<Composite, String> pair : columns)
		{
			String propertyName = pair.left.get(1, STRING_SRZ);

			if (StringUtils.equals(propertyName, PropertyType.SERIAL_VERSION_UID.name()))
			{
				if (Long.parseLong(pair.right) != entityMeta.getSerialVersionUID())
				{
					throw new IllegalStateException(
							"Saved serialVersionUID does not match current serialVersionUID for entity '"
									+ entityMeta.getClassName() + "'");
				}
				continue;
			}

			PropertyMeta<K, V> propertyMeta = (PropertyMeta<K, V>) propertyMetas.get(propertyName);

			if (propertyMeta.type() == PropertyType.SIMPLE)
			{
				setSimplePropertyToEntity(pair.right, propertyMeta, entity);
			}

			else if (propertyMeta.type() == PropertyType.LIST)
			{
				PropertyMeta<Void, V> listMeta = (PropertyMeta<Void, V>) propertyMeta;
				addToList(listProperties, listMeta, listMeta.getValueFromString(pair.right));
			}

			else if (propertyMeta.type() == PropertyType.SET)
			{
				PropertyMeta<Void, V> setMeta = (PropertyMeta<Void, V>) propertyMeta;
				addToSet(setProperties, setMeta, setMeta.getValueFromString(pair.right));
			}

			else if (propertyMeta.type() == PropertyType.MAP)
			{
				PropertyMeta<K, V> mapMeta = (PropertyMeta<K, V>) propertyMeta;

				addToMap(mapProperties, mapMeta, propertyMeta.getKeyValueFromString(pair.right));
			}
		}

		for (Entry<String, List<V>> entry : listProperties.entrySet())
		{
			setListPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
		}

		for (Entry<String, Set<V>> entry : setProperties.entrySet())
		{
			setSetPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
		}

		for (Entry<String, Map<K, V>> entry : mapProperties.entrySet())
		{
			setMapPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
		}

	}

	protected <V> void addToList(Map<String, List<V>> listProperties,
			PropertyMeta<Void, V> listMeta, V value)
	{
		String propertyName = listMeta.getPropertyName();
		List<V> list = null;
		if (!listProperties.containsKey(propertyName))
		{
			list = listMeta.newListInstance();
			listProperties.put(propertyName, list);
		}
		else
		{
			list = listProperties.get(propertyName);
		}
		list.add(value);
	}

	protected <V> void addToSet(Map<String, Set<V>> setProperties, PropertyMeta<Void, V> setMeta,
			V value)
	{
		String propertyName = setMeta.getPropertyName();

		Set<V> set = null;
		if (!setProperties.containsKey(propertyName))
		{
			set = setMeta.newSetInstance();
			setProperties.put(propertyName, set);
		}
		else
		{
			set = setProperties.get(propertyName);
		}
		set.add(value);
	}

	protected <K, V> void addToMap(Map<String, Map<K, V>> mapProperties,
			PropertyMeta<K, V> mapMeta, KeyValue<K, V> keyValue)
	{
		String propertyName = mapMeta.getPropertyName();

		Map<K, V> map = null;
		if (!mapProperties.containsKey(propertyName))
		{
			map = mapMeta.newMapInstance();
			mapProperties.put(propertyName, map);
		}
		else
		{
			map = mapProperties.get(propertyName);
		}
		map.put(keyValue.getKey(), mapMeta.castValue(keyValue.getValue()));
	}

	public <T, ID> void setIdToEntity(ID key, PropertyMeta<?, ?> idMeta, T entity)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Set primary key value {} to entity {} ", key, entity);
		}

		try
		{
			introspector.setValueToField(entity, idMeta.getSetter(), key);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + key + "' to entity " + entity, e);
		}
	}

	public <T, ID> void setSimplePropertyToEntity(String value, PropertyMeta<?, ?> propertyMeta,
			T entity)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Set simple property {} to entity {} ", propertyMeta.getPropertyName(),
					entity);
		}

		try
		{
			introspector.setValueToField(entity, propertyMeta.getSetter(),
					propertyMeta.getValueFromString(value));
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + value + "' to entity " + entity, e);
		}
	}

	public <T, ID> void setListPropertyToEntity(List<?> list, PropertyMeta<?, ?> listMeta, T entity)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Set list property {} to entity {} ", listMeta.getPropertyName(), entity);
		}

		try
		{
			introspector.setValueToField(entity, listMeta.getSetter(), list);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + list + "' to entity " + entity, e);
		}
	}

	public <T, ID> void setSetPropertyToEntity(Set<?> set, PropertyMeta<?, ?> setMeta, T entity)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Set set property {} to entity {} ", setMeta.getPropertyName(), entity);
		}

		try
		{
			introspector.setValueToField(entity, setMeta.getSetter(), set);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + set + "' to entity " + entity, e);
		}
	}

	public <T, ID> void setMapPropertyToEntity(Map<?, ?> map, PropertyMeta<?, ?> mapMeta, T entity)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Set map property {} to entity {} ", mapMeta.getPropertyName(), entity);
		}

		try
		{
			introspector.setValueToField(entity, mapMeta.getSetter(), map);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + map + "' to entity " + entity, e);
		}
	}
}
