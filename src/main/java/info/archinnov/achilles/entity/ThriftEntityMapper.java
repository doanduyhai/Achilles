package info.archinnov.achilles.entity;

import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.Pair;
import info.archinnov.achilles.exception.AchillesException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
public class ThriftEntityMapper
{

	private static final Logger log = LoggerFactory.getLogger(ThriftEntityMapper.class);

	private AchillesEntityIntrospector introspector = new AchillesEntityIntrospector();

	public void setEagerPropertiesToEntity(Object key, List<Pair<Composite, String>> columns,
			EntityMeta entityMeta, Object entity)
	{
		log.trace("Set eager properties to entity {} ", entityMeta.getClassName());

		Map<String, List<Object>> listProperties = new HashMap<String, List<Object>>();
		Map<String, Set<Object>> setProperties = new HashMap<String, Set<Object>>();
		Map<String, Map<Object, Object>> mapProperties = new HashMap<String, Map<Object, Object>>();

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

			// PropertyMeta<K, V> propertyMeta = entityMeta
			// .<K, V> getPropertyMetaByProperty(propertyName);

			PropertyMeta<?, ?> propertyMeta = propertyMetas.get(propertyName);

			if (propertyMeta.type() == PropertyType.SIMPLE)
			{
				setSimplePropertyToEntity(pair.right, propertyMeta, entity);
			}

			else if (propertyMeta.type() == PropertyType.LIST)
			{
				addToList(listProperties, propertyMeta, propertyMeta.getValueFromString(pair.right));
			}

			else if (propertyMeta.type() == PropertyType.SET)
			{
				addToSet(setProperties, propertyMeta, propertyMeta.getValueFromString(pair.right));
			}

			else if (propertyMeta.type() == PropertyType.MAP)
			{
				addToMap(mapProperties, propertyMeta,
						propertyMeta.getKeyValueFromString(pair.right));
			}
		}

		for (Entry<String, List<Object>> entry : listProperties.entrySet())
		{
			setListPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
		}

		for (Entry<String, Set<Object>> entry : setProperties.entrySet())
		{
			setSetPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
		}

		for (Entry<String, Map<Object, Object>> entry : mapProperties.entrySet())
		{
			setMapPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
		}

	}

	protected void addToList(Map<String, List<Object>> listProperties, PropertyMeta<?, ?> listMeta,
			Object value)
	{
		String propertyName = listMeta.getPropertyName();
		List<Object> list = null;
		if (!listProperties.containsKey(propertyName))
		{
			list = new ArrayList<Object>();
			listProperties.put(propertyName, list);
		}
		else
		{
			list = listProperties.get(propertyName);
		}
		list.add(value);
	}

	protected void addToSet(Map<String, Set<Object>> setProperties, PropertyMeta<?, ?> setMeta,
			Object value)
	{
		String propertyName = setMeta.getPropertyName();

		Set<Object> set = null;
		if (!setProperties.containsKey(propertyName))
		{
			set = new HashSet<Object>();
			setProperties.put(propertyName, set);
		}
		else
		{
			set = setProperties.get(propertyName);
		}
		set.add(value);
	}

	protected void addToMap(Map<String, Map<Object, Object>> mapProperties,
			PropertyMeta<?, ?> mapMeta, KeyValue<?, ?> keyValue)
	{
		String propertyName = mapMeta.getPropertyName();

		Map<Object, Object> map = null;
		if (!mapProperties.containsKey(propertyName))
		{
			map = new HashMap<Object, Object>();
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
		log.trace("Set primary key value {} to entity {} ", key, entity);

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
		log.trace("Set simple property {} to entity {} ", propertyMeta.getPropertyName(), entity);

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

	public void setListPropertyToEntity(List<?> list, PropertyMeta<?, ?> listMeta, Object entity)
	{
		log.trace("Set list property {} to entity {} ", listMeta.getPropertyName(), entity);

		try
		{
			introspector.setValueToField(entity, listMeta.getSetter(), list);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + list + "' to entity " + entity, e);
		}
	}

	public void setSetPropertyToEntity(Set<?> set, PropertyMeta<?, ?> setMeta, Object entity)
	{
		log.trace("Set set property {} to entity {} ", setMeta.getPropertyName(), entity);

		try
		{
			introspector.setValueToField(entity, setMeta.getSetter(), set);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + set + "' to entity " + entity, e);
		}
	}

	public void setMapPropertyToEntity(Map<?, ?> map, PropertyMeta<?, ?> mapMeta, Object entity)
	{
		log.trace("Set map property {} to entity {} ", mapMeta.getPropertyName(), entity);

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
