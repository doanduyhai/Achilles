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

	protected <T extends MapMeta<K, V>> T build(T meta)
	{
		Validator.validateNotNull(keyClass, "keyClass");
		super.build(meta);
		Serializer<K> keySerializer = SerializerTypeInferer.getSerializer(keyClass);
		meta.setKeyClass(keyClass);
		meta.setKeySerializer(keySerializer);

		return null;
	}

	@Override
	public MapMeta<K, V> build()
	{

		MapMeta<K, V> meta;

		if (this.lazy)
		{
			meta = new MapLazyMeta<K, V>();
		}
		else
		{
			meta = new MapMeta<K, V>();
		}

		this.build(meta);
		return meta;
	}

}
