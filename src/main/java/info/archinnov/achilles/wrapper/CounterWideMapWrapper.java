package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.helper.LoggerHelper.format;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.GenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesCounterSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.wrapper.builder.CounterWrapperBuilder;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CounterWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterWideMapWrapper<ID, K> extends AbstractWideMapWrapper<ID, K, Counter>
{

	private static Logger log = LoggerFactory.getLogger(CounterWideMapWrapper.class);

	private ID id;
	private GenericWideRowDao<ID, Long> wideMapCounterDao;
	private PropertyMeta<K, Counter> propertyMeta;

	private CompositeHelper compositeHelper;
	private KeyValueFactory keyValueFactory;
	private IteratorFactory iteratorFactory;
	private CompositeFactory compositeFactory;

	@Override
	public Counter get(K key)
	{
		log.trace("Get counter value having key {}", key);
		Composite comp = compositeFactory.createForQuery(propertyMeta, key, EQUAL);

		return CounterWrapperBuilder.builder(id) //
				.columnName(comp) //
				.counterDao(wideMapCounterDao) //
				.context(context) //
				.readLevel(propertyMeta.getReadConsistencyLevel()) //
				.writeLevel(propertyMeta.getWriteConsistencyLevel()) //
				.build();

	}

	@Override
	public void insert(K key, Counter value, int ttl)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot insert counter value with ttl");
	}

	@Override
	public void insert(K key, Counter value)
	{
		log.trace("Insert counter value {} with key {}", value, key);
		Composite comp = compositeFactory.createBaseComposite(propertyMeta, key);
		try
		{
			wideMapCounterDao.incrementCounter(id, comp, value.get());
		}
		catch (Exception e)
		{
			log.trace("Exception raised, clean up consistency levels");
			context.cleanUpFlushContext();
			throw new AchillesException(e);
		}
	}

	@Override
	public List<KeyValue<K, Counter>> find(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeFactory.createForQuery(propertyMeta, start, end, bounds,
				ordering);
		if (log.isTraceEnabled())
		{
			log.trace("Find key/value pairs in range {} / {} with bounding {} and ordering {}",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
		}

		List<HCounterColumn<Composite>> hColumns = wideMapCounterDao.findCounterColumnsRange(id,
				queryComps[0], queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createCounterKeyValueList(context, propertyMeta, hColumns);
	}

	@Override
	public List<Counter> findValues(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		if (log.isTraceEnabled())
		{
			log.trace("Find value in range {} / {} with bounding {} and ordering {}",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
		}
		List<HCounterColumn<Composite>> hColumns = wideMapCounterDao.findCounterColumnsRange(id,
				queryComps[0], queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createCounterValueList(context, propertyMeta, hColumns);
	}

	@Override
	public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);
		Composite[] queryComps = compositeFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		if (log.isTraceEnabled())
		{
			log.trace("Find keys in range {} / {} with bounding {} and ordering {}",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
		}

		List<HCounterColumn<Composite>> hColumns = wideMapCounterDao.findCounterColumnsRange(id,
				queryComps[0], queryComps[1], count, ordering.isReverse());
		return keyValueFactory.createCounterKeyList(propertyMeta, hColumns);
	}

	@Override
	public KeyValueIterator<K, Counter> iterator(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		Composite[] queryComps = compositeFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		if (log.isTraceEnabled())
		{
			log.trace(
					"Iterate in range {} / {} with bounding {} and ordering {} and batch of {} elements",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name(),
					count);
		}
		AchillesCounterSliceIterator<ID> columnSliceIterator = wideMapCounterDao
				.getCounterColumnsIterator(id, queryComps[0], queryComps[1], ordering.isReverse(),
						count);

		return iteratorFactory.createCounterKeyValueIterator(context, columnSliceIterator,
				propertyMeta);
	}

	@Override
	public void remove(K key)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void remove(K start, K end, BoundingMode bounds)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void removeFirst(int count)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void removeLast(int count)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public Counter get(K key, ConsistencyLevel readLevel)
	{
		return get(key);
	}

	@Override
	public void insert(K key, Counter value, ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public void insert(K key, Counter value, int ttl, ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot insert counter value with ttl");
	}

	@Override
	public List<KeyValue<K, Counter>> find(K start, K end, int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<KeyValue<K, Counter>> find(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<KeyValue<K, Counter>> findBoundsExclusive(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<KeyValue<K, Counter>> findReverse(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<KeyValue<K, Counter>> findReverseBoundsExclusive(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValue<K, Counter> findFirst(ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<KeyValue<K, Counter>> findFirst(int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValue<K, Counter> findLast(ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<KeyValue<K, Counter>> findLast(int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<Counter> findValues(K start, K end, int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<Counter> findValues(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<Counter> findBoundsExclusiveValues(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<Counter> findReverseValues(K start, K end, int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<Counter> findReverseBoundsExclusiveValues(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public Counter findFirstValue(ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<Counter> findFirstValues(int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public Counter findLastValue(ConsistencyLevel readLevel)
	{
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<Counter> findLastValues(int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering,
			ConsistencyLevel readLevel)
	{
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<K> findKeys(K start, K end, int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<K> findBoundsExclusiveKeys(K start, K end, int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<K> findReverseKeys(K start, K end, int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<K> findReverseBoundsExclusiveKeys(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public K findFirstKey(ConsistencyLevel readLevel)
	{
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<K> findFirstKeys(int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public K findLastKey(ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public List<K> findLastKeys(int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValueIterator<K, Counter> iterator(ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValueIterator<K, Counter> iterator(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValueIterator<K, Counter> iterator(int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValueIterator<K, Counter> iterator(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValueIterator<K, Counter> iteratorBoundsExclusive(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValueIterator<K, Counter> iteratorReverse(ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValueIterator<K, Counter> iteratorReverse(int count, ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValueIterator<K, Counter> iteratorReverse(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public KeyValueIterator<K, Counter> iteratorReverseBoundsExclusive(K start, K end, int count,
			ConsistencyLevel readLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException(
				"Please set runtime consistency level at Counter level instead of at WideMap level");
	}

	@Override
	public void remove(K key, ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void remove(K start, K end, ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void remove(K start, K end, BoundingMode bounds, ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void removeBoundsExclusive(K start, K end, ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void removeFirst(ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void removeFirst(int count, ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void removeLast(ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	@Override
	public void removeLast(int count, ConsistencyLevel writeLevel)
	{
		context.cleanUpFlushContext();
		throw new UnsupportedOperationException("Cannot remove counter value");
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setPropertyMeta(PropertyMeta<K, Counter> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
	}

	public void setCompositeHelper(CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
	}

	public void setKeyValueFactory(KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
	}

	public void setIteratorFactory(IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
	}

	public void setCompositeKeyFactory(CompositeFactory compositeFactory)
	{
		this.compositeFactory = compositeFactory;
	}

	public void setWideMapCounterDao(GenericWideRowDao<ID, Long> wideMapCounterDao)
	{
		this.wideMapCounterDao = wideMapCounterDao;
	}
}
