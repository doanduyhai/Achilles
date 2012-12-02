package fr.doan.achilles.entity.metadata.builder;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.entity.metadata.MapLazyMeta;
import fr.doan.achilles.entity.metadata.MapMeta;
import fr.doan.achilles.validation.Validator;

public class MapMetaBuilder<K, V> extends SimpleMetaBuilder<V>
{

	private Class<K> keyClass;

	public static <K, V> MapMetaBuilder<K, V> mapMetaBuilder(Class<K> keyClass, Class<V> valueClass)
	{
		return new MapMetaBuilder<K, V>(keyClass, valueClass);
	}

	public MapMetaBuilder(Class<K> keyClass, Class<V> valueClass) {
		super(valueClass);
		this.keyClass = keyClass;
	}

	@Override
	public MapMeta<K, V> build()
	{

		Validator.validateNotNull(keyClass, "keyClass");

		MapMeta<K, V> meta;

		if (this.lazy)
		{
			meta = new MapLazyMeta<K, V>();
		}
		else
		{
			meta = new MapMeta<K, V>();
		}

		super.build(meta);

		Serializer<?> keySerializer = SerializerTypeInferer.getSerializer(keyClass);
		meta.setKeyClass(keyClass);
		meta.setKeySerializer(keySerializer);

		return meta;
	}

}
