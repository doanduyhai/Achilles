package fr.doan.achilles.entity.metadata.builder;

import java.lang.reflect.Method;
import java.util.Arrays;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.entity.metadata.SimpleLazyPropertyMeta;
import fr.doan.achilles.entity.metadata.SimplePropertyMeta;
import fr.doan.achilles.validation.Validator;

public class SimplePropertyMetaBuilder<V>
{

	private String propertyName;
	private Class<V> valueClass;
	private Method[] accessors;
	protected boolean lazy;

	public static <V> SimplePropertyMetaBuilder<V> simplePropertyMetaBuilder(Class<V> valueClass)
	{
		return new SimplePropertyMetaBuilder<V>(valueClass);
	}

	public SimplePropertyMetaBuilder(Class<V> valueClass) {
		this.valueClass = valueClass;
	}

	public SimplePropertyMeta<V> build()
	{
		SimplePropertyMeta<V> meta;
		if (this.lazy)
		{
			meta = new SimpleLazyPropertyMeta<V>();
		}
		else
		{
			meta = new SimplePropertyMeta<V>();
		}
		this.build(meta);
		return meta;
	}

	protected void build(SimplePropertyMeta<V> meta)
	{
		Validator.validateNotBlank(propertyName, "propertyName");
		Validator.validateNotNull(valueClass, "valueClazz");
		Validator.validateNotNull(accessors, "accessors");
		Validator.validateSize(Arrays.asList(accessors), 2, "accessors");

		Serializer<?> valueSerializer = SerializerTypeInferer.getSerializer(valueClass);

		meta.setPropertyName(propertyName);
		meta.setValueClass(valueClass);
		meta.setValueSerializer(valueSerializer);
		meta.setGetter(accessors[0]);
		meta.setSetter(accessors[1]);
		meta.setLazy(lazy);
	}

	public SimplePropertyMetaBuilder<V> propertyName(String propertyName)
	{
		this.propertyName = propertyName;
		return this;
	}

	public SimplePropertyMetaBuilder<V> accessors(Method[] accessors)
	{
		this.accessors = accessors;
		return this;
	}

	public SimplePropertyMetaBuilder<V> lazy(boolean lazy)
	{
		this.lazy = lazy;
		return this;
	}
}
