package info.archinnov.achilles.iterator.factory;

import static info.archinnov.achilles.helper.LoggerHelper.format;
import info.archinnov.achilles.dao.GenericWideRowDao;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.wrapper.builder.CounterWrapperBuilder;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

/**
 * CompositeTransformer
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompositeTransformer
{
	private static final Logger log = LoggerFactory.getLogger(CompositeTransformer.class);

	private PropertyHelper helper = new PropertyHelper();
	private EntityProxifier proxifier = new EntityProxifier();

	public <K, V> Function<HColumn<Composite, ?>, K> buildKeyTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{
		return new Function<HColumn<Composite, ?>, K>()
		{
			public K apply(HColumn<Composite, ?> hColumn)
			{
				return buildKey(propertyMeta, hColumn);
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
				return buildKeyValue(context, propertyMeta, hColumn);
			}
		};
	}

	public <ID, K, V> KeyValue<K, V> buildKeyValue(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		K key = buildKey(propertyMeta, hColumn);
		V value = this.buildValue(context, propertyMeta, hColumn);
		int ttl = hColumn.getTtl();

		KeyValue<K, V> keyValue = new KeyValue<K, V>(key, value, ttl);
		if (log.isTraceEnabled())
		{
			log.trace("Built key/value from {} = {}",
					format(hColumn.getName()) + ":" + hColumn.getValue(), keyValue);
		}
		return keyValue;
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID, K, V> V buildValue(PersistenceContext<ID> context,
			PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		V value;
		if (propertyMeta.isJoin())
		{
			PersistenceContext<JOIN_ID> joinContext = context.newPersistenceContext(
					(EntityMeta<JOIN_ID>) propertyMeta.joinMeta(), (V) hColumn.getValue());
			value = proxifier.buildProxy((V) hColumn.getValue(), joinContext);
		}
		else
		{
			value = propertyMeta.castValue(hColumn.getValue());
		}
		if (log.isTraceEnabled())
		{
			log.trace("Built value from {} = {}",
					format(hColumn.getName()) + ":" + hColumn.getValue(), value);
		}
		return value;
	}

	public <K, V> K buildKey(PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn)
	{
		K key;
		if (propertyMeta.isSingleKey())
		{
			key = (K) hColumn.getName().get(0, propertyMeta.getKeySerializer());
		}
		else
		{
			key = helper
					.buildMultiKeyFromComposite(propertyMeta, hColumn.getName().getComponents());
		}

		if (log.isTraceEnabled())
		{
			log.trace("Built key from {} = {}", format(hColumn.getName()), key);
		}
		return key;
	}

	public <ID, K, V> Function<HCounterColumn<Composite>, KeyValue<K, Counter>> buildCounterKeyValueTransformer(
			final PersistenceContext<ID> context, final PropertyMeta<K, Counter> propertyMeta)
	{
		return new Function<HCounterColumn<Composite>, KeyValue<K, Counter>>()
		{
			@Override
			public KeyValue<K, Counter> apply(HCounterColumn<Composite> hColumn)
			{
				return buildCounterKeyValue(context, propertyMeta, hColumn);
			}
		};
	}

	public <ID, K> KeyValue<K, Counter> buildCounterKeyValue(PersistenceContext<ID> context,
			PropertyMeta<K, Counter> propertyMeta, HCounterColumn<Composite> hColumn)
	{
		K key = buildCounterKey(propertyMeta, hColumn);
		Counter value = buildCounterValue(context, propertyMeta, hColumn);

		return new KeyValue<K, Counter>(key, value, 0);
	}

	public <K> K buildCounterKey(PropertyMeta<K, ?> propertyMeta, HCounterColumn<Composite> hColumn)
	{
		K key;
		if (propertyMeta.isSingleKey())
		{
			key = hColumn.getName().get(0, propertyMeta.getKeySerializer());
		}
		else
		{
			key = helper
					.buildMultiKeyFromComposite(propertyMeta, hColumn.getName().getComponents());
		}
		if (log.isTraceEnabled())
		{
			log.trace("Built counter key from {} = {}", format(hColumn.getName()), key);
		}
		return key;
	}

	@SuppressWarnings("unchecked")
	public <ID, K> Counter buildCounterValue(PersistenceContext<ID> context,
			PropertyMeta<K, Counter> propertyMeta, HCounterColumn<Composite> hColumn)
	{
		return CounterWrapperBuilder.builder(context.getPrimaryKey()) //
				.columnName(hColumn.getName())
				//
				.counterDao(
						(GenericWideRowDao<ID, Long>) context.findWideRowDao(propertyMeta
								.getExternalCFName())) //
				.readLevel(propertyMeta.getReadConsistencyLevel()) //
				.writeLevel(propertyMeta.getWriteConsistencyLevel()) //
				.context(context)//
				.build();
	}

	public <ID, K> Function<HCounterColumn<Composite>, Counter> buildCounterValueTransformer(
			final PersistenceContext<ID> context, final PropertyMeta<K, Counter> propertyMeta)
	{
		return new Function<HCounterColumn<Composite>, Counter>()
		{
			@Override
			public Counter apply(HCounterColumn<Composite> hColumn)
			{
				return buildCounterValue(context, propertyMeta, hColumn);
			}
		};
	}

	public <K> Function<HCounterColumn<Composite>, K> buildCounterKeyTransformer(
			final PropertyMeta<K, ?> propertyMeta)
	{
		return new Function<HCounterColumn<Composite>, K>()
		{
			@Override
			public K apply(HCounterColumn<Composite> hColumn)
			{
				return buildCounterKey(propertyMeta, hColumn);
			}
		};
	}
}
