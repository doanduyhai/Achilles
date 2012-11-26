package fr.doan.achilles.entity.metadata.builder;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.entity.metadata.MapLazyPropertyMeta;
import fr.doan.achilles.entity.metadata.MapPropertyMeta;
import fr.doan.achilles.validation.Validator;

public class MapPropertyMetaBuilder<K, V> extends SimplePropertyMetaBuilder<V>
{

	private Class<K> keyClass;

	public static <K, V> MapPropertyMetaBuilder<K, V> mapPropertyMetaBuilder(Class<K> keyClass, Class<V> valueClass)
	{
		return new MapPropertyMetaBuilder<K, V>(keyClass, valueClass);
	}

	public MapPropertyMetaBuilder(Class<K> keyClass, Class<V> valueClass) {
		super(valueClass);
		this.keyClass = keyClass;
	}

	@Override
	public MapPropertyMeta<K, V> build()
	{

		Validator.validateNotNull(keyClass, "keyClass");

		MapPropertyMeta<K, V> meta;

		if (this.lazy)
		{
			meta = new MapLazyPropertyMeta<K, V>();
		}
		else
		{
			meta = new MapPropertyMeta<K, V>();
		}

		super.build(meta);

		Serializer<?> keySerializer = SerializerTypeInferer.getSerializer(keyClass);
		meta.setKeyClass(keyClass);
		meta.setKeySerializer(keySerializer);

		return meta;
	}

}
