package info.archinnov.achilles.iterator.factory;

import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import com.google.common.base.Function;

/**
 * DynamicCompositeTransformer
 * 
 * @author DuyHai DOAN
 * 
 */
public class DynamicCompositeTransformer
{

	private PropertyHelper helper = new PropertyHelper();
	private EntityHelper entityHelper = new EntityHelper();

	public <K> Function<HColumn<DynamicComposite, ?>, K> buildKeyTransformer(
			final PropertyMeta<K, ?> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, ?>, K>()
		{
			@Override
			public K apply(HColumn<DynamicComposite, ?> hColumn)
			{
				return buildKeyFromDynamicComposite(propertyMeta, hColumn);
			}
		};
	}

	public <K> Function<HCounterColumn<DynamicComposite>, K> buildCounterKeyTransformer(
			final PropertyMeta<K, ?> propertyMeta)
	{

		return new Function<HCounterColumn<DynamicComposite>, K>()
		{
			@Override
			public K apply(HCounterColumn<DynamicComposite> hColumn)
			{
				return buildCounterKeyFromDynamicComposite(propertyMeta, hColumn);
			}
		};
	}

	public <K, V> Function<HColumn<DynamicComposite, String>, V> buildValueTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, String>, V>()
		{
			@Override
			public V apply(HColumn<DynamicComposite, String> hColumn)
			{
				return propertyMeta.getValueFromString(hColumn.getValue());
			}
		};
	}

	public <K> Function<HCounterColumn<DynamicComposite>, Long> buildCounterValueTransformer(
			final PropertyMeta<K, Long> propertyMeta)
	{

		return new Function<HCounterColumn<DynamicComposite>, Long>()
		{
			@Override
			public Long apply(HCounterColumn<DynamicComposite> hColumn)
			{
				return hColumn.getValue();
			}
		};
	}

	public <K, V> Function<HColumn<DynamicComposite, String>, Object> buildRawValueTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, String>, Object>()
		{
			@Override
			public Object apply(HColumn<DynamicComposite, String> hColumn)
			{
				if (propertyMeta.type().isJoinColumn())
				{
					return propertyMeta.joinIdMeta().getValueFromString(hColumn.getValue());
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
			@Override
			public Integer apply(HColumn<DynamicComposite, String> hColumn)
			{
				return hColumn.getTtl();
			}
		};
	}

	public <ID, K, V> Function<HColumn<DynamicComposite, String>, KeyValue<K, V>> buildKeyValueTransformer(
			final PersistenceContext<ID> context, final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<DynamicComposite, String>, KeyValue<K, V>>()
		{
			@Override
			public KeyValue<K, V> apply(HColumn<DynamicComposite, String> hColumn)
			{
				return buildKeyValueFromDynamicComposite(context, propertyMeta, hColumn);
			}
		};
	}

	public <ID, K, V> KeyValue<K, V> buildKeyValueFromDynamicComposite(
			PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, String> hColumn)
	{
		K key = buildKeyFromDynamicComposite(propertyMeta, hColumn);
		V value = this.buildValueFromDynamicComposite(context, propertyMeta, hColumn);
		int ttl = hColumn.getTtl();

		return new KeyValue<K, V>(key, value, ttl);
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, K, V> V buildValueFromDynamicComposite(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<DynamicComposite, ?> hColumn)
	{
		V value;
		if (propertyMeta.isJoin())
		{
			PersistenceContext<JOIN_ID> joinContext = context.newPersistenceContext(
					(EntityMeta<JOIN_ID>) propertyMeta.joinMeta(), (V) hColumn.getValue());
			value = entityHelper.buildProxy((V) hColumn.getValue(), joinContext);
		}
		else
		{
			value = propertyMeta.getValueFromString(hColumn.getValue());
		}

		return value;
	}

	public <K> K buildKeyFromDynamicComposite(PropertyMeta<K, ?> propertyMeta,
			HColumn<DynamicComposite, ?> hColumn)
	{
		K key;
		if (propertyMeta.isSingleKey())
		{
			key = hColumn.getName().get(2, propertyMeta.getKeySerializer());
		}
		else
		{
			key = helper.buildMultiKeyForDynamicComposite(propertyMeta, hColumn.getName()
					.getComponents());
		}
		return key;
	}

	public <K, V> Function<HCounterColumn<DynamicComposite>, KeyValue<K, Long>> buildCounterKeyValueTransformer(
			final PropertyMeta<K, Long> propertyMeta)
	{

		return new Function<HCounterColumn<DynamicComposite>, KeyValue<K, Long>>()
		{
			@Override
			public KeyValue<K, Long> apply(HCounterColumn<DynamicComposite> hColumn)
			{
				return buildCounterKeyValueFromDynamicComposite(propertyMeta, hColumn);
			}
		};
	}

	public <K> KeyValue<K, Long> buildCounterKeyValueFromDynamicComposite(
			PropertyMeta<K, Long> propertyMeta, HCounterColumn<DynamicComposite> hColumn)
	{
		K key = buildCounterKeyFromDynamicComposite(propertyMeta, hColumn);
		Long value = this.buildCounterValueFromDynamicComposite(propertyMeta, hColumn);

		return new KeyValue<K, Long>(key, value, 0);
	}

	public <K> K buildCounterKeyFromDynamicComposite(PropertyMeta<K, ?> propertyMeta,
			HCounterColumn<DynamicComposite> hColumn)
	{
		K key;
		if (propertyMeta.isSingleKey())
		{
			key = hColumn.getName().get(2, propertyMeta.getKeySerializer());
		}
		else
		{
			key = helper.buildMultiKeyForDynamicComposite(propertyMeta, hColumn.getName()
					.getComponents());
		}
		return key;
	}

	public <K> Long buildCounterValueFromDynamicComposite(PropertyMeta<K, Long> propertyMeta,
			HCounterColumn<DynamicComposite> hColumn)
	{
		return propertyMeta.castValue(hColumn.getValue());
	}

}
