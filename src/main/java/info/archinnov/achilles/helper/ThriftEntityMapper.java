package info.archinnov.achilles.helper;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.type.Pair;

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
 * ThriftEntityMapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityMapper extends AchillesEntityMapper
{
	private static final Logger log = LoggerFactory.getLogger(ThriftEntityMapper.class);

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

}
