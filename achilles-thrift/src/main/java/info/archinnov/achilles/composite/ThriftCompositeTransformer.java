package info.archinnov.achilles.composite;

import static info.archinnov.achilles.logger.ThriftLoggerHelper.format;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.iterator.ThriftHColumn;
import info.archinnov.achilles.proxy.wrapper.builder.ThriftCounterWrapperBuilder;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValue;

import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

/**
 * ThriftCompositeTransformer
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCompositeTransformer
{
	private static final Logger log = LoggerFactory.getLogger(ThriftCompositeTransformer.class);

	private ThriftCompoundKeyMapper compoundKeyMapper = new ThriftCompoundKeyMapper();
	private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
	private ThriftEntityMapper mapper = new ThriftEntityMapper();

	public <K, V> Function<HColumn<Composite, ?>, K> buildKeyTransformer(
			final PropertyMeta<K, V> propertyMeta)
	{
		return new Function<HColumn<Composite, ?>, K>()
		{
			@Override
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
			@Override
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
			@Override
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
			@Override
			public Integer apply(HColumn<Composite, ?> hColumn)
			{
				return hColumn.getTtl();
			}
		};
	}

	public Function<HColumn<Composite, ?>, Long> buildTimestampTransformer()
	{
		return new Function<HColumn<Composite, ?>, Long>()
		{
			@Override
			public Long apply(HColumn<Composite, ?> hColumn)
			{
				return hColumn.getClock();
			}
		};
	}

	public <K, V> Function<HColumn<Composite, V>, KeyValue<K, V>> buildKeyValueTransformer(
			final ThriftPersistenceContext context, final PropertyMeta<K, V> propertyMeta)
	{
		return new Function<HColumn<Composite, V>, KeyValue<K, V>>()
		{
			@Override
			public KeyValue<K, V> apply(HColumn<Composite, V> hColumn)
			{
				return buildKeyValue(context, propertyMeta, hColumn);
			}
		};
	}

	public <K, V> KeyValue<K, V> buildKeyValue(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, V> hColumn)
	{
		K key = buildKey(propertyMeta, hColumn);
		V value = this.buildValue(context, propertyMeta, hColumn);
		int ttl = hColumn.getTtl();
		long timestamp = hColumn.getClock();

		KeyValue<K, V> keyValue = new KeyValue<K, V>(key, value, ttl, timestamp);
		if (log.isTraceEnabled())
		{
			log.trace("Built key/value from {} = {}",
					format(hColumn.getName()) + ":" + hColumn.getValue(), keyValue);
		}
		return keyValue;
	}

	public <K, V> V buildValue(ThriftPersistenceContext context, PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, V> hColumn)
	{
		V value = hColumn.getValue();
		if (propertyMeta.isJoin())
		{
			ThriftPersistenceContext joinContext = context.createContextForJoin(
					propertyMeta.joinMeta(), value);
			value = proxifier.buildProxy(hColumn.getValue(), joinContext);
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
		Serializer<K> keySerializer = ThriftSerializerTypeInferer.getSerializer(propertyMeta
				.getKeyClass());
		if (propertyMeta.isSingleKey())
		{
			key = hColumn.getName().get(0, keySerializer);
		}
		else
		{
			key = compoundKeyMapper.fromCompositeToCompound(propertyMeta, hColumn
					.getName()
					.getComponents());
		}

		if (log.isTraceEnabled())
		{
			log.trace("Built key from {} = {}", format(hColumn.getName()), key);
		}
		return key;
	}

	public <K, V> Function<HCounterColumn<Composite>, KeyValue<K, Counter>> buildCounterKeyValueTransformer(
			final ThriftPersistenceContext context, final PropertyMeta<K, Counter> propertyMeta)
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

	public <K> KeyValue<K, Counter> buildCounterKeyValue(ThriftPersistenceContext context,
			PropertyMeta<K, Counter> propertyMeta, HCounterColumn<Composite> hColumn)
	{
		K key = buildCounterKey(propertyMeta, hColumn);
		Counter value = buildCounterValue(context, propertyMeta, hColumn);

		return new KeyValue<K, Counter>(key, value, 0, 0);
	}

	public <K> K buildCounterKey(PropertyMeta<K, ?> propertyMeta, HCounterColumn<Composite> hColumn)
	{
		K key;
		Serializer<K> keySerializer = ThriftSerializerTypeInferer.getSerializer(propertyMeta
				.getKeyClass());
		if (propertyMeta.isSingleKey())
		{
			key = hColumn.getName().get(0, keySerializer);
		}
		else
		{
			key = compoundKeyMapper.fromCompositeToCompound(propertyMeta, hColumn
					.getName()
					.getComponents());
		}
		if (log.isTraceEnabled())
		{
			log.trace("Built counter key from {} = {}", format(hColumn.getName()), key);
		}
		return key;
	}

	public <K> Counter buildCounterValue(ThriftPersistenceContext context,
			PropertyMeta<K, Counter> propertyMeta,
			HCounterColumn<Composite> hColumn)
	{
		return ThriftCounterWrapperBuilder
				.builder(context)
				.columnName(hColumn.getName())
				.counterDao(context.findWideRowDao(propertyMeta.getExternalTableName()))
				.readLevel(propertyMeta.getReadConsistencyLevel())
				.writeLevel(propertyMeta.getWriteConsistencyLevel())
				.key(context.getPrimaryKey())
				.build();
	}

	public <K> Function<HCounterColumn<Composite>, Counter> buildCounterValueTransformer(
			final ThriftPersistenceContext context, final PropertyMeta<K, Counter> propertyMeta)
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

	// //////////////////// Clustered Entities

	public <T> Function<HColumn<Composite, Object>, T> buildClusteredEntityTransformer(
			final Class<T> entityClass,
			final ThriftPersistenceContext context)
	{
		return new Function<HColumn<Composite, Object>, T>()
		{
			@Override
			public T apply(HColumn<Composite, Object> hColumn)
			{
				return buildClusteredEntity(entityClass, context, hColumn);
			}
		};
	}

	public <T> Function<HColumn<Composite, Object>, T> buildJoinClusteredEntityTransformer(
			final Class<T> entityClass, final ThriftPersistenceContext context,
			final Map<Object, Object> joinEntitiesMap)
	{
		return new Function<HColumn<Composite, Object>, T>()
		{
			@Override
			public T apply(HColumn<Composite, Object> hColumn)
			{
				Object joinId = hColumn.getValue();
				Object clusteredValue = joinEntitiesMap.get(joinId);
				ThriftHColumn<Composite, Object> hCol = new ThriftHColumn<Composite, Object>(
						hColumn.getName(), clusteredValue);
				return buildClusteredEntity(entityClass, context, hCol);
			}
		};
	}

	public <T> Function<HCounterColumn<Composite>, T> buildCounterClusteredEntityTransformer(
			final Class<T> entityClass, final ThriftPersistenceContext context)
	{
		return new Function<HCounterColumn<Composite>, T>()
		{
			@Override
			public T apply(HCounterColumn<Composite> hColumn)
			{
				return buildCounterClusteredEntity(entityClass, context, hColumn);
			}
		};
	}

	public <T> T buildClusteredEntity(Class<T> entityClass, ThriftPersistenceContext context,
			HColumn<Composite, Object> hColumn)
	{
		PropertyMeta<?, ?> idMeta = context.getIdMeta();
		PropertyMeta<?, ?> pm = context.getFirstMeta();
		Object embeddedId = buildEmbeddedIdFromComponents(context, hColumn
				.getName()
				.getComponents());
		Object clusteredValue = hColumn.getValue();
		Object value = pm.castValue(clusteredValue);
		return mapper.createClusteredEntityWithValue(entityClass, idMeta, pm, embeddedId, value);
	}

	public <T> T buildCounterClusteredEntity(Class<T> entityClass,
			ThriftPersistenceContext context,
			HCounterColumn<Composite> hColumn)
	{
		PropertyMeta<?, ?> idMeta = context.getIdMeta();
		Object embeddedId = buildEmbeddedIdFromComponents(context, hColumn
				.getName()
				.getComponents());
		return mapper.initClusteredEntity(entityClass, idMeta, embeddedId);
	}

	private Object buildEmbeddedIdFromComponents(ThriftPersistenceContext context,
			List<Component<?>> components)
	{
		Object partitionKey = context.getPartitionKey();
		PropertyMeta<?, ?> idMeta = context.getIdMeta();
		return compoundKeyMapper.fromCompositeToEmbeddedId(idMeta, components, partitionKey);
	}

}
