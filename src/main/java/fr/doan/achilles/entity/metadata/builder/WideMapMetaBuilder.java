package fr.doan.achilles.entity.metadata.builder;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.entity.metadata.WideMapMeta;

/**
 * WideMapPropertyMetaBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapMetaBuilder<K, V>
{
	protected String propertyName;
	private Class<K> keyClass;
	private Serializer<K> keySerializer;
	private Class<V> valueClass;
	private Serializer<V> valueSerializer;

	public static <K, V> WideMapMetaBuilder<K, V> wideMapPropertyMetaBuiler(Class<K> keyClass,
			Class<V> valueClass)
	{
		return new WideMapMetaBuilder<K, V>(keyClass, valueClass);
	}

	public WideMapMetaBuilder(Class<K> keyClass, Class<V> valueClass) {
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}

	public WideMapMeta<K, V> build()
	{
		WideMapMeta<K, V> propertyMeta = new WideMapMeta<K, V>();
		build(propertyMeta);
		return propertyMeta;
	}

	protected void build(WideMapMeta<K, V> propertyMeta)
	{
		propertyMeta.setKeyClass(keyClass);
		propertyMeta.setValueClass(valueClass);
		propertyMeta.setPropertyName(propertyName);

		keySerializer = SerializerTypeInferer.getSerializer(keyClass);
		valueSerializer = SerializerTypeInferer.getSerializer(valueClass);

		propertyMeta.setKeySerializer(keySerializer);
		propertyMeta.setValueSerializer(valueSerializer);
	}

	public WideMapMetaBuilder<K, V> propertyName(String propertyName)
	{
		this.propertyName = propertyName;
		return this;
	}

}
