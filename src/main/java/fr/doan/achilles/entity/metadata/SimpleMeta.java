package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;

import me.prettyprint.hector.api.Serializer;

public class SimpleMeta<V> implements PropertyMeta<V>
{
	private String propertyName;
	private Class<V> valueClass;
	private Serializer<?> valueSerializer;
	private Method getter;
	private Method setter;
	private boolean lazy;

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.SIMPLE;
	}

	@Override
	public V get(Object object)
	{
		return valueClass.cast(object);
	}

	@Override
	public String getPropertyName()
	{
		return propertyName;
	}

	public void setPropertyName(String name)
	{
		this.propertyName = name;
	}

	@Override
	public Class<V> getValueClass()
	{
		return valueClass;
	}

	public void setValueClass(Class<V> valueClass)
	{
		this.valueClass = valueClass;
	}

	@Override
	public Serializer<?> getValueSerializer()
	{
		return valueSerializer;
	}

	public void setValueSerializer(Serializer<?> valueSerializer)
	{
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Method getGetter()
	{
		return getter;
	}

	public void setGetter(Method getter)
	{
		this.getter = getter;
	}

	@Override
	public Method getSetter()
	{
		return setter;
	}

	public void setSetter(Method setter)
	{
		this.setter = setter;
	}

	@Override
	public boolean isLazy()
	{
		return lazy;
	}

	public void setLazy(boolean lazy)
	{
		this.lazy = lazy;
	}

}
