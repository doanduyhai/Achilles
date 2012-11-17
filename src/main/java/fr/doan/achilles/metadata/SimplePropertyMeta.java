package fr.doan.achilles.metadata;

import java.io.Serializable;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.validation.Validator;

public class SimplePropertyMeta<V extends Serializable> implements PropertyMeta<V>
{
	protected String name;
	protected String valueCanonicalClassName;
	protected Class<V> valueClass;
	protected Serializer<?> valueSerializer;

	public SimplePropertyMeta(String name, Class<V> valueClazz) {
		super();
		Validator.validateNotBlank(name, "name");
		Validator.validateNotNull(valueClazz, "valueClazz");
		this.name = name;
		this.valueClass = valueClazz;
		this.bootStrapProperties();
	}

	private void bootStrapProperties()
	{
		this.valueSerializer = SerializerTypeInferer.getSerializer(valueClass);
		this.valueCanonicalClassName = this.valueClass.getCanonicalName();
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public String getValueCanonicalClassName()
	{
		return valueCanonicalClassName;
	}

	@Override
	public Class<V> getValueClass()
	{
		return valueClass;
	}

	@Override
	public Serializer<?> getValueSerializer()
	{
		return valueSerializer;
	}

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
}
