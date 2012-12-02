package fr.doan.achilles.entity.metadata.builder;

import java.lang.reflect.Method;
import java.util.Arrays;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.entity.metadata.SimpleLazyMeta;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.validation.Validator;

public class SimpleMetaBuilder<V>
{

	private String propertyName;
	private Class<V> valueClass;
	private Method[] accessors;
	protected boolean lazy;

	public static <V> SimpleMetaBuilder<V> simpleMetaBuilder(Class<V> valueClass)
	{
		return new SimpleMetaBuilder<V>(valueClass);
	}

	public SimpleMetaBuilder(Class<V> valueClass) {
		this.valueClass = valueClass;
	}

	public SimpleMeta<V> build()
	{
		SimpleMeta<V> meta;
		if (this.lazy)
		{
			meta = new SimpleLazyMeta<V>();
		}
		else
		{
			meta = new SimpleMeta<V>();
		}
		this.build(meta);
		return meta;
	}

	protected void build(SimpleMeta<V> meta)
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

	public SimpleMetaBuilder<V> propertyName(String propertyName)
	{
		this.propertyName = propertyName;
		return this;
	}

	public SimpleMetaBuilder<V> accessors(Method[] accessors)
	{
		this.accessors = accessors;
		return this;
	}

	public SimpleMetaBuilder<V> lazy(boolean lazy)
	{
		this.lazy = lazy;
		return this;
	}
}
