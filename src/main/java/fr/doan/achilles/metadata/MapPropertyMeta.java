package fr.doan.achilles.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.validation.Validator;

@SuppressWarnings("rawtypes")
public class MapPropertyMeta<V extends Serializable> extends SimplePropertyMeta<V>
{

	private Class<? extends Map> mapClass;
	private Class<? extends Serializable> keyClass;
	private Serializer<?> keyClassSerializer;

	public MapPropertyMeta(String name, Class<? extends Serializable> keyClass, Class<V> valueClass, Class<? extends Map> mapClass) {
		super(name, valueClass);
		Validator.validateNotNull(keyClass, "keyClass");
		Validator.validateNotNull(mapClass, "mapClass");
		Validator.validateNoargsConstructor(mapClass);

		this.keyClass = keyClass;
		this.keyClassSerializer = SerializerTypeInferer.getSerializer(keyClass);
		if (mapClass == Map.class)
		{
			this.mapClass = HashMap.class;
		}
		else
		{
			this.mapClass = mapClass;
		}

	}

	public Class<? extends Serializable> getKeyClass()
	{
		return this.keyClass;
	}

	public Serializer<?> getKeyClassSerializer()
	{
		return keyClassSerializer;
	}

	@SuppressWarnings("unchecked")
	public Map<? extends Serializable, V> newMapInstance() throws InstantiationException, IllegalAccessException
	{
		return this.mapClass.newInstance();
	}

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.MAP;
	}
}
