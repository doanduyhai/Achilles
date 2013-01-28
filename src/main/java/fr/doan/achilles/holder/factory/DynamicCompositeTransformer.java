package fr.doan.achilles.holder.factory;

import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import com.google.common.base.Function;

import fr.doan.achilles.entity.PropertyHelper;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.holder.KeyValue;

/**
 * TransformerBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class DynamicCompositeTransformer
{

	private PropertyHelper helper = new PropertyHelper();

	public <K, V> Function<HColumn<DynamicComposite, Object>, K> buildKeyTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, Object>, K>()
		{
			public K apply(HColumn<DynamicComposite, Object> hColumn)
			{
				return buildKeyFromDynamicComposite(propertyMeta, hColumn);
			}
		};
	}

	public <K, V> Function<HColumn<DynamicComposite, Object>, V> buildValueTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, Object>, V>()
		{
			public V apply(HColumn<DynamicComposite, Object> hColumn)
			{
				return propertyMeta.getValue(hColumn.getValue());
			}
		};
	}

	public <K, V> Function<HColumn<DynamicComposite, Object>, Object> buildRawValueTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, Object>, Object>()
		{
			public Object apply(HColumn<DynamicComposite, Object> hColumn)
			{
				return hColumn.getValue();
			}
		};
	}

	public <K, V> Function<HColumn<DynamicComposite, Object>, Integer> buildTtlTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, Object>, Integer>()
		{
			public Integer apply(HColumn<DynamicComposite, Object> hColumn)
			{
				return hColumn.getTtl();
			}
		};
	}

	public <K, V> Function<HColumn<DynamicComposite, Object>, KeyValue<K, V>> buildKeyValueTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, Object>, KeyValue<K, V>>()
		{
			public KeyValue<K, V> apply(HColumn<DynamicComposite, Object> hColumn)
			{
				return buildKeyValueFromDynamicComposite(propertyMeta, hColumn);
			}
		};
	}

	public <K, V> KeyValue<K, V> buildKeyValueFromDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		K key = buildKeyFromDynamicComposite(propertyMeta, hColumn);
		V value = propertyMeta.getValue(hColumn.getValue());
		int ttl = hColumn.getTtl();

		return new KeyValue<K, V>(key, value, ttl);
	}

	public <K, V> K buildKeyFromDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		K key;
		if (propertyMeta.isSingleKey())
		{
			key = (K) hColumn.getName().get(2, propertyMeta.getKeySerializer());
		}
		else
		{
			key = helper.buildMultiKeyForDynamicComposite(propertyMeta, hColumn.getName()
					.getComponents());
		}
		return key;
	}
}
