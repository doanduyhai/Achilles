package fr.doan.achilles.entity.metadata.factory;

import java.lang.reflect.Method;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.MultiKeyProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;

/**
 * PropertyMetaFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMetaFactory<K, V>
{
	private PropertyType type;
	private String propertyName;
	private Class<K> keyClass;
	private Class<V> valueClass;
	private Method[] accessors;

	private JoinProperties joinProperties;
	private MultiKeyProperties multiKeyProperties;

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

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public PropertyMeta<K, V> build()
	{
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
				meta = (PropertyMeta<K, V>) new PropertyMeta<Void, V>();
				break;
			case MAP:
			case LAZY_MAP:
			case WIDE_MAP:
			case JOIN_WIDE_MAP:
				meta = new PropertyMeta<K, V>();
				break;

			default:
				throw new IllegalStateException("The type '" + type
						+ "' is not supported for PropertyMeta builder");
		}

		meta.setType(type);
		meta.setPropertyName(propertyName);
		meta.setKeyClass(keyClass);
		if (keyClass != Void.class)
		{
			meta.setKeySerializer((Serializer) SerializerTypeInferer.getSerializer(keyClass));
		}
		meta.setValueClass(valueClass);
		meta.setValueSerializer((Serializer) SerializerTypeInferer.getSerializer(valueClass));
		meta.setGetter(accessors[0]);
		meta.setSetter(accessors[1]);

		meta.setJoinProperties(joinProperties);
		meta.setMultiKeyProperties(multiKeyProperties);

		meta.setSingleKey(singleKey);

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

	public PropertyMetaFactory<K, V> joinProperties(JoinProperties joinProperties)
	{
		this.joinProperties = joinProperties;
		return this;
	}

	public PropertyMetaFactory<K, V> multiKeyProperties(MultiKeyProperties multiKeyProperties)
	{
		this.multiKeyProperties = multiKeyProperties;
		return this;
	}

}
