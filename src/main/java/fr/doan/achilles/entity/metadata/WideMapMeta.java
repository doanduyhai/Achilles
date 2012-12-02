package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;

import me.prettyprint.hector.api.Serializer;

/**
 * WideMapPropertyMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapMeta<K, V> implements PropertyMeta<V>
{

	private String propertyName;
	private Class<K> keyClass;
	private Serializer<K> keySerializer;
	private Class<V> valueClass;
	private Serializer<V> valueSerializer;

	public PropertyType propertyType()
	{
		return PropertyType.WIDE_MAP;
	}

	public boolean isSingleKey()
	{
		return true;
	}

	@Override
	public String getPropertyName()
	{
		return propertyName;
	}

	public Class<K> getKeyClass()
	{
		return keyClass;
	}

	public Serializer<K> getKeySerializer()
	{
		return keySerializer;
	}

	public Class<V> getValueClass()
	{
		return valueClass;
	}

	@Override
	public Serializer<V> getValueSerializer()
	{
		return valueSerializer;
	}

	@Override
	public V get(Object object)
	{
		return valueClass.cast(object);
	}

	public K getKey(Object object)
	{
		return keyClass.cast(object);
	}

	@Override
	public Method getGetter()
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for a WideMap type");
	}

	@Override
	public Method getSetter()
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for a WideMap type");
	}

	@Override
	public boolean isLazy()
	{
		return true;
	}

	public boolean isInternal()
	{
		return true;
	}

	public void setPropertyName(String propertyName)
	{
		this.propertyName = propertyName;
	}

	public void setKeyClass(Class<K> keyClass)
	{
		this.keyClass = keyClass;
	}

	public void setKeySerializer(Serializer<K> keySerializer)
	{
		this.keySerializer = keySerializer;
	}

	public void setValueClass(Class<V> valueClass)
	{
		this.valueClass = valueClass;
	}

	public void setValueSerializer(Serializer<V> valueSerializer)
	{
		this.valueSerializer = valueSerializer;
	}
}
