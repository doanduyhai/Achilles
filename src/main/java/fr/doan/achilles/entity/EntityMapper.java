package fr.doan.achilles.entity;

import static fr.doan.achilles.serializer.SerializerUtils.STRING_SRZ;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.doan.achilles.columnFamily.ColumnFamilyBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.AchillesException;
import fr.doan.achilles.holder.KeyValue;

/**
 * EntityMapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMapper
{

	private static final Logger log = LoggerFactory.getLogger(ColumnFamilyBuilder.class);

	private EntityHelper helper = new EntityHelper();

	@SuppressWarnings("unchecked")
	public <T, ID> void setEagerPropertiesToEntity(ID key,
			List<Pair<DynamicComposite, Object>> columns, EntityMeta<ID> entityMeta, T entity)
	{

		log.trace("Set eager properties to entity {} ", entityMeta.getClassName());

		Map<String, List<?>> listProperties = new HashMap<String, List<?>>();
		Map<String, Set<?>> setProperties = new HashMap<String, Set<?>>();
		Map<String, Map<?, ?>> mapProperties = new HashMap<String, Map<?, ?>>();

		setIdToEntity(key, entityMeta.getIdMeta(), entity);

		Map<String, PropertyMeta<?, ?>> propertyMetas = entityMeta.getPropertyMetas();

		for (Pair<DynamicComposite, Object> pair : columns)
		{
			String propertyName = pair.left.get(1, STRING_SRZ);

			if (StringUtils.equals(propertyName, PropertyType.SERIAL_VERSION_UID.name()))
			{
				if (!Long.class.cast(pair.right).equals(entityMeta.getSerialVersionUID()))
				{
					throw new IllegalStateException(
							"Saved serialVersionUID does not match current serialVersionUID for entity '"
									+ entityMeta.getClassName() + "'");
				}
				continue;
			}

			PropertyMeta<?, ?> propertyMeta = propertyMetas.get(propertyName);

			if (propertyMeta.type() == PropertyType.SIMPLE)
			{
				setSimplePropertyToEntity(pair.right, propertyMeta, entity);
			}

			else if (propertyMeta.type() == PropertyType.LIST)
			{
				PropertyMeta<Void, ?> listMeta = (PropertyMeta<Void, ?>) propertyMeta;
				addToList(listProperties, listMeta, listMeta.getValue(pair.right));
			}

			else if (propertyMeta.type() == PropertyType.SET)
			{
				PropertyMeta<Void, ?> setMeta = (PropertyMeta<Void, ?>) propertyMeta;
				addToSet(setProperties, setMeta, setMeta.getValue(pair.right));
			}

			else if (propertyMeta.type() == PropertyType.MAP)
			{
				PropertyMeta<?, ?> mapMeta = (PropertyMeta<?, ?>) propertyMeta;
				addToMap(mapProperties, mapMeta, (KeyValue<?, ?>) pair.right);
			}
		}

		for (Entry<String, List<?>> entry : listProperties.entrySet())
		{
			setListPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
		}

		for (Entry<String, Set<?>> entry : setProperties.entrySet())
		{
			setSetPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
		}

		for (Entry<String, Map<?, ?>> entry : mapProperties.entrySet())
		{
			setMapPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
		}

	}

	@SuppressWarnings("unchecked")
	protected <V> void addToList(Map<String, List<?>> listProperties,
			PropertyMeta<Void, ?> listMeta, V value)
	{
		String propertyName = listMeta.getPropertyName();
		List<V> list = null;
		if (!listProperties.containsKey(propertyName))
		{
			list = (List<V>) listMeta.newListInstance();
			listProperties.put(propertyName, list);
		}
		else
		{
			list = (List<V>) listProperties.get(propertyName);
		}
		list.add(value);
	}

	@SuppressWarnings("unchecked")
	protected <V> void addToSet(Map<String, Set<?>> setProperties, PropertyMeta<Void, ?> setMeta,
			V value)
	{
		String propertyName = setMeta.getPropertyName();

		Set<V> set = null;
		if (!setProperties.containsKey(propertyName))
		{
			set = (Set<V>) setMeta.newSetInstance();
			setProperties.put(propertyName, set);
		}
		else
		{
			set = (Set<V>) setProperties.get(propertyName);
		}
		set.add(value);
	}

	@SuppressWarnings("unchecked")
	protected <K, V> void addToMap(Map<String, Map<?, ?>> mapProperties,
			PropertyMeta<K, V> mapMeta, KeyValue<?, ?> keyValue)
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
			map = (Map<K, V>) mapProperties.get(propertyName);
		}
		map.put((K) keyValue.getKey(), mapMeta.getValue(keyValue.getValue()));
	}

	public <T, ID> void setIdToEntity(ID key, PropertyMeta<?, ?> keyMeta, T entity)
	{
		log.trace("Set primary key to entity {} ", entity);
		try
		{
			helper.setValueToField(entity, keyMeta.getSetter(), key);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + key + "' to entity " + entity, e);
		}
	}

	public <T, ID> void setSimplePropertyToEntity(Object value, PropertyMeta<?, ?> propertyMeta,
			T entity)
	{
		log.trace("Set simple property {} to entity {} ", propertyMeta.getPropertyName(), entity);
		try
		{
			helper.setValueToField(entity, propertyMeta.getSetter(), propertyMeta.getValue(value));
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + value + "' to entity " + entity, e);
		}
	}

	public <T, ID> void setListPropertyToEntity(List<?> list, PropertyMeta<?, ?> listMeta, T entity)
	{
		log.trace("Set list property {} to entity {} ", listMeta.getPropertyName(), entity);
		try
		{
			helper.setValueToField(entity, listMeta.getSetter(), list);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + list + "' to entity " + entity, e);
		}
	}

	public <T, ID> void setSetPropertyToEntity(Set<?> set, PropertyMeta<?, ?> setMeta, T entity)
	{
		log.trace("Set set property {} to entity {} ", setMeta.getPropertyName(), entity);
		try
		{
			helper.setValueToField(entity, setMeta.getSetter(), set);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + set + "' to entity " + entity, e);
		}
	}

	public <T, ID> void setMapPropertyToEntity(Map<?, ?> map, PropertyMeta<?, ?> mapMeta, T entity)
	{
		log.trace("Set map property {} to entity {} ", mapMeta.getPropertyName(), entity);
		try
		{
			helper.setValueToField(entity, mapMeta.getSetter(), map);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot set value '" + map + "' to entity " + entity, e);
		}
	}
}
