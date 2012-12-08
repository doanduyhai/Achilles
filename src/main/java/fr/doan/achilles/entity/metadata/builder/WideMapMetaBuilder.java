package fr.doan.achilles.entity.metadata.builder;

import fr.doan.achilles.entity.metadata.WideMapMeta;

/**
 * WideMapPropertyMetaBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapMetaBuilder<K, V> extends MapMetaBuilder<K, V>
{

	public static <K, V> WideMapMetaBuilder<K, V> wideMapPropertyMetaBuiler(Class<K> keyClass,
			Class<V> valueClass)
	{
		return new WideMapMetaBuilder<K, V>(keyClass, valueClass);
	}

	public WideMapMetaBuilder(Class<K> keyClass, Class<V> valueClass) {
		super(keyClass, valueClass);
	}

	public WideMapMeta<K, V> build()
	{
		WideMapMeta<K, V> propertyMeta = new WideMapMeta<K, V>();
		build(propertyMeta);
		return propertyMeta;
	}

	protected <T extends WideMapMeta<K, V>> void build(T meta)
	{
		super.build(meta);
	}
}
