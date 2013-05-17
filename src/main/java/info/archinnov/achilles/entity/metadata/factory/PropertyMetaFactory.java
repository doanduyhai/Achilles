package info.archinnov.achilles.entity.metadata.factory;

import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Pair;

import java.lang.reflect.Method;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyMetaFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMetaFactory<K, V>
{
	private static final Logger log = LoggerFactory.getLogger(PropertyMetaFactory.class);

	private PropertyType type;
	private String propertyName;
	private String entityClassName;
	private Class<K> keyClass;
	private Class<V> valueClass;
	private Method[] accessors;
	private ObjectMapper objectMapper;
	private CounterProperties counterProperties;

	private JoinProperties joinProperties;
	private MultiKeyProperties multiKeyProperties;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

	public PropertyMetaFactory(Class<K> keyClass, Class<V> valueClass) {
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}

	public static <K, V> PropertyMetaFactory<K, V> factory(Class<K> keyClass, Class<V> valueClass)
	{
		return new PropertyMetaFactory<K, V>(keyClass, valueClass);
	}

	public static <V> PropertyMetaFactory<Void, V> factory(Class<V> valueClass)
	{
		return new PropertyMetaFactory<Void, V>(Void.class, valueClass);
	}

	public PropertyMetaFactory<K, V> propertyName(String propertyName)
	{
		this.propertyName = propertyName;
		return this;
	}

	public PropertyMetaFactory<K, V> entityClassName(String entityClassName)
	{
		this.entityClassName = entityClassName;
		return this;
	}

	public PropertyMetaFactory<K, V> objectMapper(ObjectMapper objectMapper)
	{
		this.objectMapper = objectMapper;
		return this;
	}

	@SuppressWarnings("unchecked")
	public PropertyMeta<K, V> build()
	{
		log.debug("Build propertyMeta for property {} of entity class {}", propertyName,
				entityClassName);

		PropertyMeta<K, V> meta = null;
		boolean singleKey = multiKeyProperties == null ? true : false;
		switch (type)
		{
			case SIMPLE:
			case LIST:
			case SET:
			case LAZY_SIMPLE:
			case LAZY_LIST:
			case LAZY_SET:
			case JOIN_SIMPLE:
			case COUNTER:
				meta = (PropertyMeta<K, V>) new PropertyMeta<Void, V>();
				break;
			case MAP:
			case LAZY_MAP:
			case WIDE_MAP:
			case JOIN_WIDE_MAP:
			case COUNTER_WIDE_MAP:
				meta = new PropertyMeta<K, V>();
				break;

			default:
				throw new IllegalStateException("The type '" + type
						+ "' is not supported for PropertyMeta builder");
		}

		meta.setObjectMapper(objectMapper);
		meta.setType(type);
		meta.setPropertyName(propertyName);
		meta.setEntityClassName(entityClassName);
		meta.setKeyClass(keyClass);
		meta.setValueClass(valueClass);
		meta.setGetter(accessors[0]);
		meta.setSetter(accessors[1]);

		meta.setJoinProperties(joinProperties);
		meta.setMultiKeyProperties(multiKeyProperties);

		meta.setSingleKey(singleKey);
		meta.setCounterProperties(counterProperties);
		meta.setConsistencyLevels(consistencyLevels);

		return meta;
	}

	public PropertyMetaFactory<K, V> type(PropertyType type)
	{
		this.type = type;
		return this;
	}

	public PropertyMetaFactory<K, V> accessors(Method[] accessors)
	{
		this.accessors = accessors;
		return this;
	}

	public PropertyMetaFactory<K, V> multiKeyProperties(MultiKeyProperties multiKeyProperties)
	{
		this.multiKeyProperties = multiKeyProperties;
		return this;
	}

	public PropertyMetaFactory<K, V> counterProperties(CounterProperties counterProperties)
	{
		this.counterProperties = counterProperties;
		return this;
	}

	public PropertyMetaFactory<K, V> consistencyLevels(
			Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		this.consistencyLevels = consistencyLevels;
		return this;
	}

}
