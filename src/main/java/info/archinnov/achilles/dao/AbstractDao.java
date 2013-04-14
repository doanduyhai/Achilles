package info.archinnov.achilles.dao;

import static me.prettyprint.hector.api.factory.HFactory.*;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.entity.execution_context.SafeExecutionContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.AchillesCounterSliceIterator;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.validation.Validator;

import java.util.Iterator;
import java.util.List;

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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * AbstractDao
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AbstractDao<K, V>
{

	protected Keyspace keyspace;
	protected Cluster cluster;
	protected Serializer<K> keySerializer;
	protected Serializer<Composite> columnNameSerializer;
	protected Serializer<V> valueSerializer;
	protected String columnFamily;
	protected AchillesConfigurableConsistencyLevelPolicy policy;

	public static int DEFAULT_LENGTH = 100;

	protected AbstractDao() {}

	protected AbstractDao(Cluster cluster, Keyspace keyspace) {
		Validator.validateNotNull(cluster, "Cluster should not be null");
		Validator.validateNotNull(keyspace, "keyspace should not be null");
		this.cluster = cluster;
		this.keyspace = keyspace;
	}

	private <T> T reinitConsistencyLevels(SafeExecutionContext<T> context)
	{
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

	protected Function<HColumn<Composite, V>, Composite> hColumnToName = new Function<HColumn<Composite, V>, Composite>()
	{
		@Override
		public Composite apply(HColumn<Composite, V> hColumn)
		{
			return hColumn.getName();
		}
	};

	public void insertColumnBatch(K key, Composite name, V value, Mutator<K> mutator)
	{
		mutator.addInsertion(key, columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer));
	}

	public V getValue(final K key, final Composite name)
	{
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
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.setValueBatch(key, name, value, mutator);
		this.executeMutator(mutator);
	}

	public void setValueBatch(K key, Composite name, V value, Mutator<K> mutator)
	{
		mutator.addInsertion(key, columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer));
	}

	public void setValue(K key, Composite name, V value, int ttl)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.setValueBatch(key, name, value, ttl, mutator);
		this.executeMutator(mutator);
	}

	public void setValueBatch(K key, Composite name, V value, int ttl, Mutator<K> mutator)
	{
		mutator.addInsertion(
				key,
				columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer).setTtl(
						ttl));
	}

	public void removeColumn(K key, Composite name)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.removeColumnBatch(key, name, mutator);
		this.executeMutator(mutator);
	}

	public void removeColumnBatch(K key, Composite name, Mutator<K> mutator)
	{
		mutator.addDeletion(key, columnFamily, name, columnNameSerializer);
	}

	public void removeColumnRange(K key, Composite start, Composite end)
	{
		this.removeColumnRange(key, start, end, false, Integer.MAX_VALUE);
	}

	public void removeColumnRange(K key, Composite start, Composite end, boolean reverse, int count)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		List<HColumn<Composite, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(start, end, reverse, count).execute().get().getColumns();

		for (HColumn<Composite, V> column : columns)
		{
			mutator.addDeletion(key, columnFamily, column.getName(), columnNameSerializer);
		}
		this.executeMutator(mutator);
	}

	public void removeColumnRangeBatch(K key, Composite start, Composite end, Mutator<K> mutator)
	{
		this.removeColumnRangeBatch(key, start, end, false, Integer.MAX_VALUE, mutator);
	}

	public void removeColumnRangeBatch(K key, Composite start, Composite end, boolean reverse,
			int count, Mutator<K> mutator)
	{
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

	public List<Pair<Composite, V>> findColumnsRange(final K key, final Composite startName,
			final Composite endName, final boolean reverse, final int count)
	{
		this.policy.loadConsistencyLevelForRead(columnFamily);
		List<HColumn<Composite, V>> columns = reinitConsistencyLevels(new SafeExecutionContext<List<HColumn<Composite, V>>>()
		{
			@Override
			public List<HColumn<Composite, V>> execute()
			{
				return createSliceQuery(keyspace, keySerializer, columnNameSerializer,
						valueSerializer).setColumnFamily(columnFamily).setKey(key)
						.setRange(startName, endName, reverse, count).execute().get().getColumns();
			}
		});
		return Lists.transform(columns, hColumnToPair);
	}

	public List<HColumn<Composite, V>> findRawColumnsRange(final K key, final Composite startName,
			final Composite endName, final int count, final boolean reverse)
	{
		this.policy.loadConsistencyLevelForRead(columnFamily);
		return reinitConsistencyLevels(new SafeExecutionContext<List<HColumn<Composite, V>>>()
		{
			@Override
			public List<HColumn<Composite, V>> execute()
			{
				return createSliceQuery(keyspace, keySerializer, columnNameSerializer,
						valueSerializer).setColumnFamily(columnFamily).setKey(key)
						.setRange(startName, endName, reverse, count).execute().get().getColumns();
			}
		});
	}

	public List<HCounterColumn<Composite>> findCounterColumnsRange(final K key,
			final Composite startName, final Composite endName, final int count,
			final boolean reverse)
	{
		this.policy.loadConsistencyLevelForRead(columnFamily);
		return reinitConsistencyLevels(new SafeExecutionContext<List<HCounterColumn<Composite>>>()
		{
			@Override
			public List<HCounterColumn<Composite>> execute()
			{
				return HFactory
						.createCounterSliceQuery(keyspace, keySerializer, columnNameSerializer)
						.setColumnFamily(columnFamily).setKey(key)
						.setRange(startName, endName, reverse, count).execute().get().getColumns();
			}
		});
	}

	public AchillesSliceIterator<K, V> getColumnsIterator(K key, Composite startName,
			Composite endName, boolean reverse, int length)
	{
		SliceQuery<K, Composite, V> query = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key);

		return new AchillesSliceIterator<K, V>(policy, columnFamily, query, startName, endName,
				reverse, length);
	}

	public AchillesCounterSliceIterator<K> getCounterColumnsIterator(K key, Composite startName,
			Composite endName, boolean reverse, int length)
	{
		this.policy.loadConsistencyLevelForRead(columnFamily);
		SliceCounterQuery<K, Composite> query = createCounterSliceQuery(keyspace, keySerializer,
				columnNameSerializer).setColumnFamily(columnFamily).setKey(key);

		return new AchillesCounterSliceIterator<K>(policy, columnFamily, query, startName, endName,
				reverse, length);
	}

	public <JOIN_ID, KEY, VALUE> AchillesJoinSliceIterator<K, V, JOIN_ID, KEY, VALUE> getJoinColumnsIterator(
			GenericEntityDao<JOIN_ID> joinEntityDao, PropertyMeta<KEY, VALUE> propertyMeta, K key,
			Composite startName, Composite endName, boolean reversed, int count)
	{
		SliceQuery<K, Composite, V> query = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key);

		return new AchillesJoinSliceIterator<K, V, JOIN_ID, KEY, VALUE>(policy, joinEntityDao,
				columnFamily, propertyMeta, query, startName, endName, reversed, count);
	}

	public Rows<K, Composite, V> multiGetSliceRange(final List<K> keys, final Composite startName,
			final Composite endName, final boolean reverse, final int size)
	{
		this.policy.loadConsistencyLevelForRead(columnFamily);
		return reinitConsistencyLevels(new SafeExecutionContext<Rows<K, Composite, V>>()
		{
			@Override
			public Rows<K, Composite, V> execute()
			{
				return HFactory
						.createMultigetSliceQuery(keyspace, keySerializer, columnNameSerializer,
								valueSerializer).setColumnFamily(columnFamily).setKeys(keys)
						.setRange(startName, endName, reverse, size).execute().get();
			}
		});
	}

	public void removeRow(K key)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.removeRowBatch(key, mutator);
		this.executeMutator(mutator);
	}

	public void removeRowBatch(K key, Mutator<K> mutator)
	{
		mutator.addDeletion(key, columnFamily);
	}

	public void insertCounterBatch(K key, Composite name, Long value, Mutator<K> mutator)
	{
		Long currentValue = this.getCounterValue(key, name);
		long delta = value - currentValue;
		mutator.addCounter(key, columnFamily,
				HFactory.createCounterColumn(name, delta, columnNameSerializer));
	}

	public void insertCounter(K key, Composite name, Long value)
	{
		Mutator<K> mutator = buildMutator();
		Long currentValue = this.getCounterValue(key, name);
		long delta = value - currentValue;
		mutator.addCounter(key, columnFamily,
				HFactory.createCounterColumn(name, delta, columnNameSerializer));
		this.executeMutator(mutator);
	}

	public void removeCounter(K key, Composite name)
	{
		Mutator<K> mutator = buildMutator();
		mutator.deleteCounter(key, columnFamily, name, columnNameSerializer);
		this.executeMutator(mutator);
	}

	public void removeCounterBatch(K key, Composite name, Mutator<K> mutator)
	{
		mutator.deleteCounter(key, columnFamily, name, columnNameSerializer);
	}

	public void removeCounterRow(K key)
	{
		SliceCounterQuery<K, Composite> query = HFactory
				.createCounterSliceQuery(keyspace, keySerializer, columnNameSerializer)
				.setColumnFamily(columnFamily).setKey(key);

		AchillesCounterSliceIterator<K> iterator = new AchillesCounterSliceIterator<K>(policy,
				columnFamily, query, (Composite) null, (Composite) null, false, DEFAULT_LENGTH);

		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		while (iterator.hasNext())
		{
			HCounterColumn<Composite> counterCol = iterator.next();
			mutator.deleteCounter(key, columnFamily, counterCol.getName(), columnNameSerializer);
		}
		this.executeMutator(mutator);
	}

	public void removeCounterRowBatch(K key, Mutator<K> mutator)
	{
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
		cluster.truncate(keyspace.getKeyspaceName(), CounterDao.COUNTER_CF);
	}

	public long getCounterValue(K key, Composite name)
	{
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

	public Mutator<K> buildMutator()
	{
		return HFactory.createMutator(this.keyspace, this.keySerializer);
	}

	public void executeMutator(final Mutator<K> mutator)
	{
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

	public void setPolicy(AchillesConfigurableConsistencyLevelPolicy policy)
	{
		this.policy = policy;
	}
}
