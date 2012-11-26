package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;

import me.prettyprint.hector.api.Serializer;

public interface PropertyMeta<V>
{

	public PropertyType propertyType();

	public String getPropertyName();

	public Class<V> getValueClass();

	public Serializer<?> getValueSerializer();

	public V get(Object object);

	public Method getGetter();

	public Method getSetter();

	public boolean isLazy();
}
