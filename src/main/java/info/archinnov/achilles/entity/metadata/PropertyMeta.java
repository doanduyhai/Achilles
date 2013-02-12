package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.holder.KeyValue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.Serializer;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyMeta<K, V>
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMeta<K, V>
{
	private ObjectMapper objectMapper;
	private PropertyType type;
	private String propertyName;
	private Class<K> keyClass;
	private Serializer<K> keySerializer;
	private Class<V> valueClass;
	private Serializer<V> valueSerializer;
	private Method getter;
	private Method setter;

	private JoinProperties joinProperties;
	private MultiKeyProperties multiKeyProperties;
	private ExternalWideMapProperties<?> externalWideMapProperties;

	private boolean singleKey;

	PropertyHelper propertyHelper = new PropertyHelper();

	private static final Logger logger = LoggerFactory.getLogger(PropertyMeta.class);

	public PropertyType type()
	{
		return type;
	}

	public void setType(PropertyType propertyType)
	{
		this.type = propertyType;
	}

	public List<V> newListInstance()
	{
		return new ArrayList<V>();
	}

	public Set<V> newSetInstance()
	{
		return new HashSet<V>();
	}

	public Map<K, V> newMapInstance()
	{
		return new HashMap<K, V>();
	}

	public String getPropertyName()
	{
		return propertyName;
	}

	public void setPropertyName(String propertyName)
	{
		this.propertyName = propertyName;
	}

	public Class<K> getKeyClass()
	{
		return keyClass;
	}

	public void setKeyClass(Class<K> keyClass)
	{
		this.keyClass = keyClass;
	}

	public Serializer<K> getKeySerializer()
	{
		return keySerializer;
	}

	public void setKeySerializer(Serializer<K> keySerializer)
	{
		this.keySerializer = keySerializer;
	}

	public Class<V> getValueClass()
	{
		return valueClass;
	}

	public void setValueClass(Class<V> valueClass)
	{
		this.valueClass = valueClass;
	}

	public Serializer<V> getValueSerializer()
	{
		return valueSerializer;
	}

	public void setValueSerializer(Serializer<V> valueSerializer)
	{
		this.valueSerializer = valueSerializer;
	}

	public Method getGetter()
	{
		return getter;
	}

	public void setGetter(Method getter)
	{
		this.getter = getter;
	}

	public Method getSetter()
	{
		return setter;
	}

	public void setSetter(Method setter)
	{
		this.setter = setter;
	}

	public MultiKeyProperties getMultiKeyProperties()
	{
		return multiKeyProperties;
	}

	public void setMultiKeyProperties(MultiKeyProperties multiKeyProperties)
	{
		this.multiKeyProperties = multiKeyProperties;
	}

	public K getKey(Object object)
	{
		return keyClass.cast(object);
	}

	public V getValueFromString(Object object)
	{
		try
		{
			if (valueClass == String.class)
			{
				return valueClass.cast(object);
			}
			else
			{
				return this.objectMapper.readValue((String) object, this.valueClass);
			}
		}
		catch (Exception e)
		{
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public KeyValue<K, V> getKeyValueFromString(Object object)
	{
		try
		{
			return (KeyValue<K, V>) this.objectMapper.readValue((String) object, KeyValue.class);

		}
		catch (Exception e)
		{
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	public String writeValueToString(Object value)
	{
		try
		{
			if (valueClass == String.class && type != MAP && type != LAZY_MAP)
			{
				return (String) value;
			}
			else
			{
				return this.objectMapper.writeValueAsString(value);
			}
		}
		catch (Exception e)
		{
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	public Object writeValueAsSupportedTypeOrString(V value)
	{
		try
		{
			if (propertyHelper.isSupportedType(valueClass))
			{
				return value;
			}
			else
			{
				return this.objectMapper.writeValueAsString(value);
			}
		}
		catch (Exception e)
		{
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	public V castValue(Object object)
	{
		try
		{
			if (type.isJoinColumn())
			{
				return this.valueClass.cast(object);
			}
			else if (propertyHelper.isSupportedType(valueClass))
			{
				return this.valueClass.cast(object);
			}
			else
			{
				return objectMapper.readValue((String) object, valueClass);
			}

		}
		catch (Exception e)
		{
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	public JoinProperties getJoinProperties()
	{
		return joinProperties;
	}

	public void setJoinProperties(JoinProperties joinProperties)
	{
		this.joinProperties = joinProperties;
	}

	public ExternalWideMapProperties<?> getExternalWideMapProperties()
	{
		return externalWideMapProperties;
	}

	public void setExternalWideMapProperties(ExternalWideMapProperties<?> externalWideMapProperties)
	{
		this.externalWideMapProperties = externalWideMapProperties;
	}

	public boolean isSingleKey()
	{
		return singleKey;
	}

	public void setSingleKey(boolean singleKey)
	{
		this.singleKey = singleKey;
	}

	public void setObjectMapper(ObjectMapper objectMapper)
	{
		this.objectMapper = objectMapper;
	}
}
