package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.PropertyHelper.isSupportedType;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.KeyValue;

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
	private CounterProperties counterProperties;
	private JoinProperties joinProperties;
	private MultiKeyProperties multiKeyProperties;
	private ExternalWideMapProperties<?> externalWideMapProperties;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

	private boolean singleKey;

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
			logger.error("Error while trying to deserialize the JSON : " + (String) object, e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public KeyValue<K, V> getKeyValueFromString(Object object)
	{
		try
		{
			return this.objectMapper.readValue((String) object, KeyValue.class);
		}
		catch (Exception e)
		{
			logger.error("Error while trying to deserialize the JSON : " + (String) object, e);
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
			logger.error("Error while trying to serialize to JSON the object : " + value, e);
			return null;
		}
	}

	public Object writeValueAsSupportedTypeOrString(V value)
	{
		try
		{
			if (isSupportedType(valueClass))
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
			logger.error("Error while trying to serialize to JSON the object : " + value, e);
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
			else if (isSupportedType(valueClass))
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
			logger.error("Error while trying to cast the object " + object + " to type '"
					+ this.valueClass.getCanonicalName() + "'", e);
			return null;
		}
	}

	public boolean isJoin()
	{
		return type.isJoinColumn();
	}

	public EntityMeta<?> joinMeta()
	{
		return joinProperties != null ? joinProperties.getEntityMeta() : null;
	}

	public PropertyMeta<Void, ?> joinIdMeta()
	{
		return joinMeta() != null ? joinMeta().getIdMeta() : null;
	}

	public PropertyMeta<Void, ?> counterIdMeta()
	{
		return counterProperties != null ? counterProperties.getIdMeta() : null;
	}

	public String fqcn()
	{
		return counterProperties != null ? counterProperties.getFqcn() : null;
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

	public CounterProperties getCounterProperties()
	{
		return counterProperties;
	}

	public void setCounterProperties(CounterProperties counterProperties)
	{
		this.counterProperties = counterProperties;
	}

	public boolean isExternal()
	{
		return this.type.isExternal();
	}

	public boolean isJoinColumn()
	{
		return this.type.isJoinColumn();
	}

	public boolean isLazy()
	{
		return this.type.isLazy();
	}

	public boolean isWideMap()
	{
		return this.type.isWideMap();
	}

	public boolean isCounter()
	{
		return this.type == COUNTER || this.type == WIDE_MAP_COUNTER;
	}

	public ConsistencyLevel getReadConsistencyLevel()
	{
		return consistencyLevels != null ? consistencyLevels.left : null;
	}

	public ConsistencyLevel getWriteConsistencyLevel()
	{
		return consistencyLevels != null ? consistencyLevels.right : null;
	}

	public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		this.consistencyLevels = consistencyLevels;
	}

}
