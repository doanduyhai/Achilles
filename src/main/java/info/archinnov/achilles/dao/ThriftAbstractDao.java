package info.archinnov.achilles.dao;

import static info.archinnov.achilles.helper.LoggerHelper.format;
import static me.prettyprint.hector.api.factory.HFactory.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.context.execution.SafeExecutionContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.AchillesCounterSliceIterator;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.serializer.SerializerUtils;
import info.archinnov.achilles.validation.Validator;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.model.HCounterColumnImpl;
import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.SliceCounterQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * AbstractDao
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class ThriftAbstractDao<K, V>
{
	public static final String LOGGER_NAME = "ACHILLES_DAO";
	private static final Logger log = LoggerFactory.getLogger(LOGGER_NAME);

	protected Keyspace keyspace;
	protected Cluster cluster;
	protected Serializer<K> keySerializer;
	protected Serializer<Composite> columnNameSerializer;
	protected Serializer<V> valueSerializer;
	protected String columnFamily;
	protected AchillesConsistencyLevelPolicy policy;

	public static int DEFAULT_LENGTH = 100;

	protected ThriftAbstractDao() {}

	protected ThriftAbstractDao(Cluster cluster, Keyspace keyspace,
			AchillesConsistencyLevelPolicy policy)
	{
		Validator.validateNotNull(cluster, "Cluster should not be null");
		Validator.validateNotNull(keyspace, "keyspace should not be null");
		Validator.validateNotNull(keyspace, "policy should not be null");
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.policy = policy;
	}

	private <T> T reinitConsistencyLevels(SafeExecutionContext<T> context)
	{
		log.trace("Execute safely and reinit consistency level in thread {}",
				Thread.currentThread());
		try
		{
			return context.execute();
		}
		finally
		{
			this.policy.reinitDefaultConsistencyLevels();
		}
	}

	protected Function<HColumn<Composite, V>, V> hColumnToValue = new Function<HColumn<Composite, V>, V>()
	{
		@Override
		public V apply(HColumn<Composite, V> hColumn)
		{
			return hColumn.getValue();
		}
	};

	protected Function<HColumn<Composite, V>, Pair<Composite, V>> hColumnToPair = new Function<HColumn<Composite, V>, Pair<Composite, V>>()
	{
		@Override
		public Pair<Composite, V> apply(HColumn<Composite, V> hColumn)
		{
			return new Pair<Composite, V>(hColumn.getName(), hColumn.getValue());
		}
	};

	public void insertColumnBatch(K key, Composite name, V value, Mutator<K> mutator)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Insert column {} into column family {} with key {}", format(name),
					columnFamily, key);
		}

		mutator.addInsertion(key, columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer));
	}

	public V getValue(final K key, final Composite name)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Get value from column family {} with key {} and column name {}",
					columnFamily, key, format(name));
		}

		this.policy.loadConsistencyLevelForRead(columnFamily);
		V result = null;
		HColumn<Composite, V> column = reinitConsistencyLevels(new SafeExecutionContext<HColumn<Composite, V>>()
		{
			@Override
			public HColumn<Composite, V> execute()
			{
				return HFactory
						.createColumnQuery(keyspace, keySerializer, columnNameSerializer,
								valueSerializer).setColumnFamily(columnFamily).setKey(key)
						.setName(name).execute().get();
			}
		});

		if (column != null)
		{
			result = column.getValue();
		}
		return result;
	}

	public void setValue(K key, Composite name, V value)
	{
		log.trace("Set value {} to column family {} with key {} , column name {}", value,
				columnFamily, key, name);

		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.setValueBatch(key, name, value, mutator);
		this.executeMutator(mutator);
	}

	public void setValueBatch(K key, Composite name, V value, Mutator<K> mutator)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Set value {} as batch mutation to column family {} with key {} , column name {}",
					value, columnFamily, key, format(name));
		}
		mutator.addInsertion(key, columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer));
	}

	public void setValueBatch(K key, Composite name, V value, int ttl, Mutator<K> mutator)
	{
		log.trace(
				"Set value {} as batch mutation to column family {} with key {} , column name {} and ttl {}",
				value, columnFamily, key, name, ttl);
		mutator.addInsertion(
				key,
				columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer).setTtl(
						ttl));
	}

	public void removeColumnBatch(K key, Composite name, Mutator<K> mutator)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Remove column name {} as batch mutation from column family {} with key {} ",
					format(name), columnFamily, key);
		}
		mutator.addDeletion(key, columnFamily, name, columnNameSerializer);
	}

	public void removeColumnRangeBatch(K key, Composite start, Composite end, Mutator<K> mutator)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Remove column slice within range having inclusive start/end {}/{} column names as batch mutation from column family {} with key {} ",
					format(start), format(end), columnFamily, key);
		}
		this.removeColumnRangeBatch(key, start, end, false, Integer.MAX_VALUE, mutator);
	}

	public void removeColumnRangeBatch(K key, Composite start, Composite end, boolean reverse,
			int count, Mutator<K> mutator)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Remove {} columns slice within range having inclusive start/end {}/{} column names as batch mutation from column family {} with key {} and reserver {}",
					count, format(start), format(end), columnFamily, key, reverse);
		}
		List<HColumn<Composite, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(start, end, reverse, count).execute().get().getColumns();

		for (HColumn<Composite, V> column : columns)
		{
			mutator.addDeletion(key, columnFamily, column.getName(), columnNameSerializer);
		}
	}

	public List<V> findValuesRange(final K key, final Composite start, final Composite end,
			final boolean reverse, final int count)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Find {} values slice within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {}",
					count, format(start), format(end), columnFamily, key, reverse);
		}
		this.policy.loadConsistencyLevelForRead(columnFamily);
		List<HColumn<Composite, V>> columns = reinitConsistencyLevels(new SafeExecutionContext<List<HColumn<Composite, V>>>()
		{
			@Override
			public List<HColumn<Composite, V>> execute()
			{
				return createSliceQuery(keyspace, keySerializer, columnNameSerializer,
						valueSerializer).setColumnFamily(columnFamily).setKey(key)
						.setRange(start, end, reverse, count).execute().get().getColumns();
			}
		});
		return Lists.transform(columns, hColumnToValue);
	}

	public List<Pair<Composite, V>> findColumnsRange(final K key, final Composite start,
			final Composite end, final boolean reverse, final int count)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Find {} columns slice within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {}",
					count, format(start), format(end), columnFamily, key, reverse);
		}
		this.policy.loadConsistencyLevelForRead(columnFamily);
		List<HColumn<Composite, V>> columns = reinitConsistencyLevels(new SafeExecutionContext<List<HColumn<Composite, V>>>()
		{
			@Override
			public List<HColumn<Composite, V>> execute()
			{
				return createSliceQuery(keyspace, keySerializer, columnNameSerializer,
						valueSerializer).setColumnFamily(columnFamily).setKey(key)
						.setRange(start, end, reverse, count).execute().get().getColumns();
			}
		});
		return Lists.transform(columns, hColumnToPair);
	}

	public List<HColumn<Composite, V>> findRawColumnsRange(final K key, final Composite start,
			final Composite end, final int count, final boolean reverse)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Find raw {} columns slice within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {}",
					count, format(start), format(end), columnFamily, key, reverse);
		}

		this.policy.loadConsistencyLevelForRead(columnFamily);
		return reinitConsistencyLevels(new SafeExecutionContext<List<HColumn<Composite, V>>>()
		{
			@Override
			public List<HColumn<Composite, V>> execute()
			{
				return createSliceQuery(keyspace, keySerializer, columnNameSerializer,
						valueSerializer).setColumnFamily(columnFamily).setKey(key)
						.setRange(start, end, reverse, count).execute().get().getColumns();
			}
		});
	}

	public List<HCounterColumn<Composite>> findCounterColumnsRange(final K key,
			final Composite start, final Composite end, final int count, final boolean reverse)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Find {} counter columns slice within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {}",
					count, format(start), format(end), columnFamily, key, reverse);
		}

		this.policy.loadConsistencyLevelForRead(columnFamily);
		return reinitConsistencyLevels(new SafeExecutionContext<List<HCounterColumn<Composite>>>()
		{
			@Override
			public List<HCounterColumn<Composite>> execute()
			{
				return HFactory
						.createCounterSliceQuery(keyspace, keySerializer, columnNameSerializer)
						.setColumnFamily(columnFamily).setKey(key)
						.setRange(start, end, reverse, count).execute().get().getColumns();
			}
		});
	}

	public AchillesSliceIterator<K, V> getColumnsIterator(K key, Composite start, Composite end,
			boolean reverse, int length)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Get columns slice iterator within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {} by batch of {} elements",
					format(start), format(end), columnFamily, key, reverse, length);
		}

		SliceQuery<K, Composite, V> query = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key);

		return new AchillesSliceIterator<K, V>(policy, columnFamily, query, start, end, reverse,
				length);
	}

	public AchillesCounterSliceIterator<K> getCounterColumnsIterator(K key, Composite start,
			Composite end, boolean reverse, int length)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Get counter columns slice iterator within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {} by batch of {} elements",
					format(start), format(end), columnFamily, key, reverse, length);
		}

		this.policy.loadConsistencyLevelForRead(columnFamily);
		SliceCounterQuery<K, Composite> query = createCounterSliceQuery(keyspace, keySerializer,
				columnNameSerializer).setColumnFamily(columnFamily).setKey(key);

		return new AchillesCounterSliceIterator<K>(policy, columnFamily, query, start, end,
				reverse, length);
	}

	public <JOIN_ID, KEY, VALUE> AchillesJoinSliceIterator<K, V, JOIN_ID, KEY, VALUE> getJoinColumnsIterator(
			ThriftGenericEntityDao<JOIN_ID> joinEntityDao, PropertyMeta<KEY, VALUE> propertyMeta,
			K key, Composite start, Composite end, boolean reversed, int count)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Get join columns iterator within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {} by batch of {} elements; for property {}",
					format(start), format(end), columnFamily, key, reversed, count,
					propertyMeta.getPropertyName());
		}

		SliceQuery<K, Composite, V> query = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key);

		return new AchillesJoinSliceIterator<K, V, JOIN_ID, KEY, VALUE>(policy, joinEntityDao,
				columnFamily, propertyMeta, query, start, end, reversed, count);
	}

	public Rows<K, Composite, V> multiGetSliceRange(final List<K> keys, final Composite start,
			final Composite end, final boolean reverse, final int size)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Multi get columns slice within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {} by batch of {} elements; for property {}",
					format(start), format(end), columnFamily, StringUtils.join(keys, ","), reverse,
					size);
		}

		this.policy.loadConsistencyLevelForRead(columnFamily);
		return reinitConsistencyLevels(new SafeExecutionContext<Rows<K, Composite, V>>()
		{
			@Override
			public Rows<K, Composite, V> execute()
			{
				return HFactory
						.createMultigetSliceQuery(keyspace, keySerializer, columnNameSerializer,
								valueSerializer).setColumnFamily(columnFamily).setKeys(keys)
						.setRange(start, end, reverse, size).execute().get();
			}
		});
	}

	public void removeRowBatch(K key, Mutator<K> mutator)
	{
		log.trace("Remove row as batch mutation from column family {} with key {}", columnFamily,
				key);

		mutator.addDeletion(key, columnFamily);
	}

	public void incrementCounter(K key, Composite name, Long value)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Incrementing counter column {} with key {} from column family {} by {}",
					format(name), key, columnFamily, value);
		}
		Mutator<K> mutator = buildMutator();
		mutator.addCounter(key, columnFamily, new HCounterColumnImpl<Composite>(name, value,
				SerializerUtils.COMPOSITE_SRZ));
		executeMutator(mutator);
	}

	public void decrementCounter(K key, Composite name, Long value)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Decrementing counter column {} with key {} from column family {} by {}",
					format(name), key, columnFamily, value);
		}
		Mutator<K> mutator = buildMutator();
		mutator.addCounter(key, columnFamily, new HCounterColumnImpl<Composite>(name, value * -1L,
				SerializerUtils.COMPOSITE_SRZ));
		executeMutator(mutator);
	}

	public long getCounterValue(K key, Composite name)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Get counter value column {} with key {} from column family {}",
					format(name), key, columnFamily);
		}

		final CounterQuery<K, Composite> counter = new ThriftCounterColumnQuery<K, Composite>(
				keyspace, keySerializer, columnNameSerializer).setColumnFamily(columnFamily)
				.setKey(key).setName(name);

		this.policy.loadConsistencyLevelForRead(columnFamily);
		return reinitConsistencyLevels(new SafeExecutionContext<Long>()
		{
			@Override
			public Long execute()
			{
				long counterValue = 0;
				HCounterColumn<Composite> column = counter.execute().get();
				if (column != null)
				{
					counterValue = column.getValue();
				}
				return counterValue;
			}
		});
	}

	public void removeCounterBatch(K key, Composite name, Mutator<K> mutator)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Remove counter column {} as batch mutation with key {} from column family {}",
					format(name), key, columnFamily);
		}

		mutator.deleteCounter(key, columnFamily, name, columnNameSerializer);
	}

	public void removeCounterRowBatch(K key, Mutator<K> mutator)
	{
		log.trace("Remove counter row as batch mutation with key {} from column family {}", key,
				columnFamily);

		SliceCounterQuery<K, Composite> query = HFactory
				.createCounterSliceQuery(keyspace, keySerializer, columnNameSerializer)
				.setColumnFamily(columnFamily).setKey(key);

		AchillesCounterSliceIterator<K> iterator = new AchillesCounterSliceIterator<K>(policy,
				columnFamily, query, (Composite) null, (Composite) null, false, DEFAULT_LENGTH);

		while (iterator.hasNext())
		{
			HCounterColumn<Composite> counterCol = iterator.next();
			mutator.deleteCounter(key, columnFamily, counterCol.getName(), columnNameSerializer);
		}
	}

	public void truncate()
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		Iterator<K> iterator = new KeyIterator<K>(keyspace, columnFamily, keySerializer).iterator();
		while (iterator.hasNext())
		{
			this.removeRowBatch(iterator.next(), mutator);
		}
		this.executeMutator(mutator);
	}

	public void truncateCounters()
	{
		cluster.truncate(keyspace.getKeyspaceName(), ThriftCounterDao.COUNTER_CF);
	}

	public Mutator<K> buildMutator()
	{
		return HFactory.createMutator(this.keyspace, this.keySerializer);
	}

	public void executeMutator(final Mutator<K> mutator)
	{
		log.trace("Execute safely mutator with {} mutations for column family {}",
				mutator.getPendingMutationCount(), columnFamily);

		this.policy.loadConsistencyLevelForWrite(this.columnFamily);
		reinitConsistencyLevels(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				mutator.execute();
				return null;
			}
		});
	}

	public String getColumnFamily()
	{
		return columnFamily;
	}
}
