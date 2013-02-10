package info.archinnov.achilles.holder.factory;

import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.holder.KeyValue;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import com.google.common.base.Function;

/**
 * TransformerBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class DynamicCompositeTransformer
{

	private PropertyHelper helper = new PropertyHelper();

	public <K, V> Function<HColumn<DynamicComposite, String>, K> buildKeyTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, String>, K>()
		{
			public K apply(HColumn<DynamicComposite, String> hColumn)
			{
				return buildKeyFromDynamicComposite(propertyMeta, hColumn);
			}
		};
	}

	public <K, V> Function<HColumn<DynamicComposite, String>, V> buildValueTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, String>, V>()
		{
			public V apply(HColumn<DynamicComposite, String> hColumn)
			{
				return propertyMeta.getValueFromString(hColumn.getValue());
			}
		};
	}

	public <K, V> Function<HColumn<DynamicComposite, String>, Object> buildRawValueTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, String>, Object>()
		{
			public Object apply(HColumn<DynamicComposite, String> hColumn)
			{
				if (propertyMeta.type().isJoinColumn())
				{
					return propertyMeta.getJoinProperties().getEntityMeta().getIdMeta()
							.getValueFromString(hColumn.getValue());
				}
				else
				{
					return hColumn.getValue();
				}
			}
		};
	}

	public Function<HColumn<DynamicComposite, String>, Integer> buildTtlTransformer()
	{

		return new Function<HColumn<DynamicComposite, String>, Integer>()
		{
			public Integer apply(HColumn<DynamicComposite, String> hColumn)
			{
				return hColumn.getTtl();
			}
		};
	}

	public <K, V> Function<HColumn<DynamicComposite, String>, KeyValue<K, V>> buildKeyValueTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, String>, KeyValue<K, V>>()
		{
			public KeyValue<K, V> apply(HColumn<DynamicComposite, String> hColumn)
			{
				return buildKeyValueFromDynamicComposite(propertyMeta, hColumn);
			}
		};
	}

	public <K, V> KeyValue<K, V> buildKeyValueFromDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, String> hColumn)
	{
		K key = buildKeyFromDynamicComposite(propertyMeta, hColumn);
		V value = this.buildValueFromDynamicComposite(propertyMeta, hColumn);
		int ttl = hColumn.getTtl();

		return new KeyValue<K, V>(key, value, ttl);
	}

	@SuppressWarnings("unchecked")
	public <K, V> V buildValueFromDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, String> hColumn)
	{
		V value;
		if (propertyMeta.type().isJoinColumn())
		{
			value = (V) hColumn.getValue();
		}
		else
		{
			value = propertyMeta.getValueFromString(hColumn.getValue());
		}

		return value;
	}

	public <K, V> K buildKeyFromDynamicComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, String> hColumn)
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
