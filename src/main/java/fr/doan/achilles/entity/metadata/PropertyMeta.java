package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;

import me.prettyprint.hector.api.Serializer;

public abstract class PropertyMeta<K, V>
{

	private String propertyName;
	private Class<K> keyClass;
	private Serializer<?> keySerializer;
	private Class<V> valueClass;
	private Serializer<?> valueSerializer;
	private Method getter;
	private Method setter;

	private JoinProperties joinProperties;
	private MultiKeyProperties multiKeyProperties;

	public abstract PropertyType propertyType();

	public abstract boolean isSingleKey();

	public abstract boolean isLazy();

	public abstract boolean isJoinColumn();

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

	public Serializer<?> getKeySerializer()
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

	public Serializer<?> getValueSerializer()
	{
		return valueSerializer;
	}

	public void setValueSerializer(Serializer<?> valueSerializer)
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

	public V getValue(Object object)
	{
		return this.valueClass.cast(object);
	}

	public JoinProperties getJoinProperties()
	{
		return joinProperties;
	}

	public void setJoinProperties(JoinProperties joinProperties)
	{
		this.joinProperties = joinProperties;
	}
}
