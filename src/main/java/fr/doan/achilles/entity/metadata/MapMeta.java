package fr.doan.achilles.entity.metadata;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Serializer;

public class MapMeta<K, V> extends SimpleMeta<V>
{

	private Class<K> keyClass;
	private Serializer<?> keySerializer;

	public Class<K> getKeyClass()
	{
		return keyClass;
	}

	public Serializer<?> getKeySerializer()
	{
		return keySerializer;
	}

	public void setKeyClass(Class<K> keyClass)
	{
		this.keyClass = keyClass;
	}

	public void setKeySerializer(Serializer<?> keyClassSerializer)
	{
		this.keySerializer = keyClassSerializer;
	}

	public Map<K, V> newMapInstance()
	{
		return new HashMap<K, V>();
	}

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.MAP;
	}
}
