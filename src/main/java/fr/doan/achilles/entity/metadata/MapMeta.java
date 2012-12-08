package fr.doan.achilles.entity.metadata;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Serializer;

public class MapMeta<K, V> extends SimpleMeta<V>
{

	private Class<K> keyClass;
	private Serializer<K> keySerializer;

	public Class<K> getKeyClass()
	{
		return keyClass;
	}

	public K getKey(Object object)
	{
		return keyClass.cast(object);
	}

	public Serializer<K> getKeySerializer()
	{
		return keySerializer;
	}

	public void setKeyClass(Class<K> keyClass)
	{
		this.keyClass = keyClass;
	}

	public void setKeySerializer(Serializer<K> K)
	{
		this.keySerializer = K;
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
