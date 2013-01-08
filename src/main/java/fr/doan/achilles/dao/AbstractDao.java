package fr.doan.achilles.dao;

import static me.prettyprint.hector.api.factory.HFactory.createCounterSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.SliceCounterQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.utils.Pair;

import fr.doan.achilles.serializer.Utils;
import fr.doan.achilles.validation.Validator;

public abstract class AbstractDao<K, N, V>
{

	protected Keyspace keyspace;
	protected Serializer<K> keySerializer;
	protected Serializer<N> columnNameSerializer;
	protected Serializer<V> valueSerializer;
	protected String columnFamily;

	protected int DEFAULT_LENGTH = 50;

	protected AbstractDao() {}

	protected AbstractDao(Keyspace keyspace) {
		Validator.validateNotNull(keyspace, "keyspace");
		this.keyspace = keyspace;
	}

	public void insertName(K key, N name)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		mutator.insert(key, columnFamily,
				HFactory.createColumn(name, null, columnNameSerializer, Utils.OBJECT_SRZ));
		mutator.execute();
	}

	public void insertNameBatch(K key, N name, Mutator<K> mutator)
	{
		mutator.insert(key, columnFamily,
				HFactory.createColumn(name, null, columnNameSerializer, Utils.OBJECT_SRZ));
	}

	public void insertColumn(K key, N name, V value, int ttl, Mutator<K> mutator)
	{
		Mutator<K> mut = mutator;
		if (mutator == null)
		{
			mut = HFactory.createMutator(keyspace, keySerializer);
		}
		mut.insert(
				key,
				columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer).setTtl(
						ttl));
		if (mutator == null)
		{
			mut.execute();
		}
	}

	public void insertColumn(K key, N name, V value, Mutator<K> mutator)
	{
		Mutator<K> mut = mutator;
		if (mutator == null)
		{
			mut = HFactory.createMutator(keyspace, keySerializer);
		}
		mut.insert(key, columnFamily,
				HFactory.createColumn(name, value, columnNameSerializer, valueSerializer));
		if (mutator == null)
		{
			mut.execute();
		}
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
		HFactory.createMutator(keyspace, keySerializer)
				.addInsertion(key, columnFamily,
						HFactory.createColumn(name, value, columnNameSerializer, valueSerializer))
				.execute();
	}

	public void setValue(K key, N name, V value, int ttl)
	{
		HFactory.createMutator(keyspace, keySerializer)
				.addInsertion(
						key,
						columnFamily,
						HFactory.createColumn(name, value, columnNameSerializer, valueSerializer)
								.setTtl(ttl)).execute();
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
		mutator.delete(key, columnFamily, name, columnNameSerializer);
		mutator.execute();
	}

	public void removeColumnRange(K key, N start, N end)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		List<HColumn<N, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(start, end, false, Integer.MAX_VALUE).execute().get().getColumns();

		for (HColumn<N, V> column : columns)
		{
			mutator.delete(key, columnFamily, column.getName(), columnNameSerializer);
		}
		mutator.execute();
	}

	public List<V> findValuesRange(K key, N startName, boolean reverse, int count)
	{
		List<V> values = new ArrayList<V>();

		List<HColumn<N, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(startName, null, reverse, count).execute().get().getColumns();

		for (HColumn<N, V> column : columns)
		{
			values.add(column.getValue());
		}
		return values;
	}

	public List<N> findNamesRange(K key, N startName, boolean reverse, int count)
	{
		List<HColumn<N, V>> columns = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(startName, null, reverse, count).execute().get().getColumns();

		List<N> names = new ArrayList<N>();
		for (HColumn<N, V> column : columns)
		{
			if (column.getValue() != null)
			{
				names.add(column.getName());
			}
		}
		return names;
	}

	public List<Pair<N, V>> findColumnsRange(K key, N startName, boolean reverse, int count)
	{
		return this.findColumnsRange(key, startName, (N) null, reverse, count);
	}

	public List<Pair<N, V>> findColumnsRange(K key, N startName, N endName, boolean reverse,
			int count)
	{
		List<HColumn<N, V>> results = createSliceQuery(keyspace, keySerializer,
				columnNameSerializer, valueSerializer).setColumnFamily(columnFamily).setKey(key)
				.setRange(startName, endName, reverse, count).execute().get().getColumns();

		List<Pair<N, V>> columns = new ArrayList<Pair<N, V>>();
		for (HColumn<N, V> column : results)
		{
			columns.add(new Pair<N, V>(column.getName(), column.getValue()));
		}

		return columns;
	}

	public List<HColumn<N, V>> findRawColumnsRange(K key, N startName, N endName, boolean reverse,
			int count)
	{
		return createSliceQuery(keyspace, keySerializer, columnNameSerializer, valueSerializer)
				.setColumnFamily(columnFamily).setKey(key)
				.setRange(startName, endName, reverse, count).execute().get().getColumns();
	}

	public ColumnSliceIterator<K, N, V> getColumnsIterator(K key, N startName, boolean reverse,
			int length)
	{
		return getColumnsIterator(key, startName, null, reverse, length);
	}

	public ColumnSliceIterator<K, N, V> getColumnsIterator(K key, N startName, N endName,
			boolean reverse)
	{
		return getColumnsIterator(key, startName, null, reverse, DEFAULT_LENGTH);
	}

	public ColumnSliceIterator<K, N, V> getColumnsIterator(K key, N startName, N endName,
			boolean reverse, int length)
	{
		SliceQuery<K, N, V> query = createSliceQuery(keyspace, keySerializer, columnNameSerializer,
				valueSerializer).setColumnFamily(columnFamily).setKey(key);

		return new ColumnSliceIterator<K, N, V>(query, startName, endName, reverse, length);
	}

	public CounterColumnSliceIterator<K, N> getCounterColumnsIterator(K key, N startName,
			boolean reverse, int length)
	{
		SliceCounterQuery<K, N> query = createCounterSliceQuery(keyspace, keySerializer,
				columnNameSerializer).setColumnFamily(columnFamily).setKey(key);

		return new CounterColumnSliceIterator<K, N>(query, startName, (N) null, reverse, length);
	}

	public <KEY, NAME, VALUE> ColumnSliceIterator<KEY, NAME, VALUE> getSpecificColumnsIterator(
			Serializer<KEY> keySz, Serializer<NAME> nameSz, Serializer<VALUE> valueSz, String CF,
			KEY key, NAME startName, boolean reverse, int length)
	{
		SliceQuery<KEY, NAME, VALUE> query = createSliceQuery(keyspace, keySz, nameSz, valueSz)
				.setColumnFamily(CF).setKey(key);

		return new ColumnSliceIterator<KEY, NAME, VALUE>(query, startName, (NAME) null, reverse,
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

	public void removeRow(K key)
	{
		Mutator<K> mutator = HFactory.createMutator(keyspace, keySerializer);
		mutator.addDeletion(key, columnFamily);
		mutator.execute();
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

	public void truncate()
	{
		Iterator<K> iterator = new KeyIterator<K>(keyspace, columnFamily, keySerializer).iterator();
		while (iterator.hasNext())
		{
			this.removeRow(iterator.next());
		}
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
