package info.archinnov.achilles.iterator.factory;

import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import com.google.common.base.Function;

/**
 * CompositeTransformer
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompositeTransformer
{

	private PropertyHelper helper = new PropertyHelper();
	private EntityHelper entityHelper = new EntityHelper();

	public <K, V> Function<HColumn<Composite, ?>, K> buildKeyTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<Composite, ?>, K>()
		{
			public K apply(HColumn<Composite, ?> hColumn)
			{
				return buildKeyFromComposite(propertyMeta, hColumn);
			}
		};
	}

	public <K, V> Function<HColumn<Composite, ?>, V> buildValueTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<Composite, ?>, V>()
		{
			public V apply(HColumn<Composite, ?> hColumn)
			{
				return propertyMeta.castValue(hColumn.getValue());
			}
		};
	}

	public Function<HColumn<Composite, ?>, ?> buildRawValueTransformer()
	{

		return new Function<HColumn<Composite, ?>, Object>()
		{
			public Object apply(HColumn<Composite, ?> hColumn)
			{
				return hColumn.getValue();
			}
		};
	}

	public Function<HColumn<Composite, ?>, Integer> buildTtlTransformer()
	{

		return new Function<HColumn<Composite, ?>, Integer>()
		{
			public Integer apply(HColumn<Composite, ?> hColumn)
			{
				return hColumn.getTtl();
			}
		};
	}

	public <ID, K, V> Function<HColumn<Composite, ?>, KeyValue<K, V>> buildKeyValueTransformer(
			final PersistenceContext<ID> context, final PropertyMeta<K, V> propertyMeta)
	{

		return new Function<HColumn<Composite, ?>, KeyValue<K, V>>()
		{
			public KeyValue<K, V> apply(HColumn<Composite, ?> hColumn)
			{
				return buildKeyValueFromComposite(context, propertyMeta, hColumn);
			}
		};
	}

	public <ID, K, V> KeyValue<K, V> buildKeyValueFromComposite(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		K key = buildKeyFromComposite(propertyMeta, hColumn);
		V value = this.buildValueFromComposite(context, propertyMeta, hColumn);
		int ttl = hColumn.getTtl();

		return new KeyValue<K, V>(key, value, ttl);
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, K, V> V buildValueFromComposite(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
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

			value = propertyMeta.castValue(hColumn.getValue());
		}

		return value;
	}

	public <K, V> K buildKeyFromComposite(PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, ?> hColumn)
	{
		K key;
		if (propertyMeta.isSingleKey())
		{
			key = (K) hColumn.getName().get(0, propertyMeta.getKeySerializer());
		}
		else
		{
			key = helper.buildMultiKeyForComposite(propertyMeta, hColumn.getName().getComponents());
		}
		return key;
	}
}
