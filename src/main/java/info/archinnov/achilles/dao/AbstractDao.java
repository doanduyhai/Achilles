package info.archinnov.achilles.dao;

import static me.prettyprint.hector.api.factory.HFactory.createCounterSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.CounterColumnSliceIterator;
import info.archinnov.achilles.serializer.SerializerUtils;
import info.archinnov.achilles.validation.Validator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.ColumnSlice;
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
public abstract class AbstractDao<K, N extends AbstractComposite, V>
{

	protected Keyspace keyspace;
	protected Serializer<K> keySerializer;
	protected Serializer<N> columnNameSerializer;
	protected Serializer<V> valueSerializer;
	protected String columnFamily;

	public static int DEFAULT_LENGTH = 50;

	protected Function<HColumn<N, V>, V> hColumnToValue = new Function<HColumn<N, V>, V>()
	{
		public V apply(HColumn<N, V> hColumn)
		{
			return hColumn.getValue();
		}
	};

	protected Function<HColumn<N, V>, Pair<N, V>> hColumnToPair = new Function<HColumn<N, V>, Pair<N, V>>()
	{
		public Pair<N, V> apply(HColumn<N, V> hColumn)
		{
			return new Pair<N, V>(hColumn.getName(), hColumn.getValue());
		}
	};

	protected Function<HColumn<N, V>, N> hColumnToName = new Function<HColumn<N, V>, N>()
	{
		public N apply(HColumn<N, V> hColumn)
		{
			return hColumn.getName();
		}
	};

	protected AbstractDao() {}

	protected AbstractDao(Keyspace keyspace) {
		Validator.validateNotNull(keyspace, "keyspace should not be null");
		this.keyspace = keyspace;
	}

	public void insertName(K key, N name)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.insertNameBatch(key, name, mutator);
		mutator.execute();
	}

	public void insertNameBatch(K key, N name, Mutator<K> mutator)
	{
		mutator.addInsertion(key, columnFamily,
				HFactory.createColumn(name, null, columnNameSerializer, SerializerUtils.OBJECT_SRZ));
	}

	public void insertColumnBatch(K key, N name, V value, int ttl, Mutator<K> mutator)
	{
		mutator.addInsertion(
				key,
				columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer).setTtl(
						ttl));
	}

	public void insertColumnBatch(K key, N name, V value, Mutator<K> mutator)
	{
		mutator.addInsertion(key, columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer));
	}

	public V getValue(K key, N name)
	{
		V result = null;
		HColumn<N, V> column = HFactory
				.createColumnQuery(keyspace, keySerializer, columnNameSerializer, valueSerializer)
				.setColumnFamily(columnFamily).setKey(key).setName(name).execute().get();
		if (column != null)
		{
			result = column.getValue();
		}
		return result;
	}

	public void setValue(K key, N name, V value)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.setValueBatch(key, name, value, mutator);
		mutator.execute();
	}

	public void setValueBatch(K key, N name, V value, Mutator<K> mutator)
	{
		mutator.addInsertion(key, columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer));
	}

	public void setValue(K key, N name, V value, int ttl)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.setValueBatch(key, name, value, ttl, mutator);
		mutator.execute();
	}

	public void setValueBatch(K key, N name, V value, int ttl, Mutator<K> mutator)
	{
		mutator.addInsertion(
				key,
				columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer).setTtl(
						ttl));
	}

	@SuppressWarnings("unchecked")
	public List<HColumn<N, V>> getColumns(K key, List<N> names)
	{
		N[] columnsName = (N[]) names.toArray();
		List<HColumn<N, V>> columns = new ArrayList<HColumn<N, V>>();
		ColumnSlice<N, V> slices = HFactory
				.createSliceQuery(keyspace, keySerializer, columnNameSerializer, valueSerializer)
				.setColumnFamily(columnFamily).setKey(key).setColumnNames(columnsName).execute()
				.get();

		if (slices.getColumns() != null && slices.getColumns().size() > 0)
		{
			columns = slices.getColumns();
		}

		return columns;
	}

	public void removeColumn(K key, N name)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.removeColumnBatch(key, name, mutator);
		mutator.execute();
	}

	public void removeColumnBatch(K key, N name, Mutator<K> mutator)
	{
		mutator.addDeletion(key, columnFamily, name, columnNameSerializer);
	}

	public void removeColumnRange(K key, N start, N end)
	{
		this.removeColumnRange(key, start, end, false, Integer.MAX_VALUE);
	}

	public void removeColumnRange(K key, N start, N end, boolean reverse, int count)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		List<HColumn<N, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(start, end, reverse, count).execute().get().getColumns();

		for (HColumn<N, V> column : columns)
		{
			mutator.addDeletion(key, columnFamily, column.getName(), columnNameSerializer);
		}
		mutator.execute();
	}

	public void removeColumnRangeBatch(K key, N start, N end, Mutator<K> mutator)
	{
		this.removeColumnRangeBatch(key, start, end, false, Integer.MAX_VALUE, mutator);
	}

	public void removeColumnRangeBatch(K key, N start, N end, boolean reverse, int count,
			Mutator<K> mutator)
	{
		List<HColumn<N, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(start, end, reverse, count).execute().get().getColumns();

		for (HColumn<N, V> column : columns)
		{
			mutator.addDeletion(key, columnFamily, column.getName(), columnNameSerializer);
		}
	}

	public List<V> findValuesRange(K key, N start, N end, boolean reverse, int count)
	{

		List<HColumn<N, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(start, end, reverse, count).execute().get().getColumns();

		return Lists.transform(columns, hColumnToValue);
	}

	public List<N> findNamesRange(K key, N startName, boolean reverse, int count)
	{
		List<HColumn<N, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(startName, null, reverse, count).execute().get().getColumns();

		return Lists.transform(columns, hColumnToName);
	}

	public List<Pair<N, V>> findColumnsRange(K key, N startName, boolean reverse, int count)
	{
		return this.findColumnsRange(key, startName, (N) null, reverse, count);
	}

	public List<Pair<N, V>> findColumnsRange(K key, N startName, N endName, boolean reverse,
			int count)
	{
		List<HColumn<N, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(startName, endName, reverse, count).execute().get().getColumns();

		return Lists.transform(columns, hColumnToPair);
	}

	public List<HColumn<N, V>> findRawColumnsRange(K key, N startName, N endName, int count,
			boolean reverse)
	{
		return createSliceQuery(keyspace, keySerializer, columnNameSerializer, valueSerializer)
				.setColumnFamily(columnFamily).setKey(key)
				.setRange(startName, endName, reverse, count).execute().get().getColumns();
	}

	public AchillesSliceIterator<K, N, V> getColumnsIterator(K key, N startName, boolean reverse,
			int length)
	{
		return getColumnsIterator(key, startName, null, reverse, length);
	}

	public AchillesSliceIterator<K, N, V> getColumnsIterator(K key, N startName, N endName,
			boolean reverse)
	{
		return getColumnsIterator(key, startName, null, reverse, DEFAULT_LENGTH);
	}

	public AchillesSliceIterator<K, N, V> getColumnsIterator(K key, N startName, N endName,
			boolean reverse, int length)
	{
		SliceQuery<K, N, V> query = createSliceQuery(keyspace, keySerializer, columnNameSerializer,
				valueSerializer).setColumnFamily(columnFamily).setKey(key);

		return new AchillesSliceIterator<K, N, V>(query, startName, endName, reverse, length);
	}

	public <KEY, VALUE> AchillesJoinSliceIterator<K, N, V, KEY, VALUE> getJoinColumnsIterator(
			PropertyMeta<KEY, VALUE> propertyMeta, K key, N startName, boolean reverse, int length)
	{
		return getJoinColumnsIterator(propertyMeta, key, startName, null, reverse, length);
	}

	public <KEY, VALUE> AchillesJoinSliceIterator<K, N, V, KEY, VALUE> getJoinColumnsIterator(
			PropertyMeta<KEY, VALUE> propertyMeta, K key, N startName, N endName, boolean reverse)
	{
		return getJoinColumnsIterator(propertyMeta, key, startName, null, reverse, DEFAULT_LENGTH);
	}

	public <KEY, VALUE> AchillesJoinSliceIterator<K, N, V, KEY, VALUE> getJoinColumnsIterator(
			PropertyMeta<KEY, VALUE> propertyMeta, K key, N startName, N endName, boolean reversed,
			int count)
	{
		SliceQuery<K, N, V> query = createSliceQuery(keyspace, keySerializer, columnNameSerializer,
				valueSerializer).setColumnFamily(columnFamily).setKey(key);

		return new AchillesJoinSliceIterator<K, N, V, KEY, VALUE>(propertyMeta, query, startName,
				endName, reversed, count);
	}

	public CounterColumnSliceIterator<K, N> getCounterColumnsIterator(K key, N startName,
			boolean reverse, int length)
	{
		SliceCounterQuery<K, N> query = createCounterSliceQuery(keyspace, keySerializer,
				columnNameSerializer).setColumnFamily(columnFamily).setKey(key);

		return new CounterColumnSliceIterator<K, N>(query, startName, (N) null, reverse, length);
	}

	public <KEY, NAME extends AbstractComposite, VALUE> AchillesSliceIterator<KEY, NAME, VALUE> getSpecificColumnsIterator(
			Serializer<KEY> keySz, Serializer<NAME> nameSz, Serializer<VALUE> valueSz, String CF,
			KEY key, NAME startName, boolean reverse, int length)
	{
		SliceQuery<KEY, NAME, VALUE> query = createSliceQuery(keyspace, keySz, nameSz, valueSz)
				.setColumnFamily(CF).setKey(key);

		return new AchillesSliceIterator<KEY, NAME, VALUE>(query, startName, (NAME) null, reverse,
				length);
	}

	public List<HCounterColumn<N>> findCounterColumnsRange(K key, N startName, boolean reverse,
			int size)
	{
		SliceCounterQuery<K, N> counterQuery = createCounterSliceQuery(keyspace, keySerializer,
				columnNameSerializer).setColumnFamily(columnFamily).setKey(key);

		return counterQuery.setRange(startName, (N) null, reverse, size).execute().get()
				.getColumns();
	}

	public Rows<K, N, V> multiGetSliceRange(List<K> keys, N startName, N endName, boolean reverse,
			int size)
	{
		return HFactory
				.createMultigetSliceQuery(keyspace, keySerializer, columnNameSerializer,
						valueSerializer).setColumnFamily(columnFamily).setKeys(keys)
				.setRange(startName, endName, reverse, size).execute().get();
	}

	public void removeRow(K key)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		this.removeRowBatch(key, mutator);
		mutator.execute();
	}

	public void removeRowBatch(K key, Mutator<K> mutator)
	{
		mutator.addDeletion(key, columnFamily);
	}

	public void incrementCounter(K key, N name, Long value)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		mutator.incrementCounter(key, columnFamily, name, value);
		mutator.execute();
	}

	public void decrementCounter(K key, N name, Long value)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		mutator.decrementCounter(key, columnFamily, name, value);
		mutator.execute();

	}

	public void insertCounter(K key, N name, Long value)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		mutator.insertCounter(key, columnFamily,
				HFactory.createCounterColumn(name, value, columnNameSerializer));
		mutator.execute();
	}

	public void truncate()
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		Iterator<K> iterator = new KeyIterator<K>(keyspace, columnFamily, keySerializer).iterator();
		while (iterator.hasNext())
		{
			this.removeRowBatch(iterator.next(), mutator);
		}
		mutator.execute();
	}

	public long getCounterValue(K key, N name)
	{
		CounterQuery<K, N> counter = new ThriftCounterColumnQuery<K, N>(keyspace, keySerializer,
				columnNameSerializer);

		counter.setColumnFamily(columnFamily).setKey(key).setName(name);

		HCounterColumn<N> column = counter.execute().get();

		if (column == null)
		{
			return 0;
		}
		else
		{
			return column.getValue();
		}
	}

	public Mutator<K> buildMutator()
	{
		return HFactory.createMutator(this.keyspace, this.keySerializer);
	}

	public String getColumnFamily()
	{
		return columnFamily;
	}
}
