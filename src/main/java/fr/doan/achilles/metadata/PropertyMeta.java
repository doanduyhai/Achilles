package fr.doan.achilles.metadata;

import java.io.Serializable;

import me.prettyprint.hector.api.Serializer;

public interface PropertyMeta<T extends Serializable>
{

	public PropertyType propertyType();

	public String getName();

	public Class<T> getValueClass();

	public String getValueCanonicalClassName();

	public Serializer<?> getValueSerializer();

	public T get(Object object);
}