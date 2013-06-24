package info.archinnov.achilles.proxy.wrapper;

import static info.archinnov.achilles.logger.ThriftLoggerHelper.format;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.ThriftCounterSliceIterator;
import info.archinnov.achilles.proxy.wrapper.builder.ThriftCounterWrapperBuilder;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;
import java.util.List;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftCounterWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCounterWideMapWrapper<K> extends ThriftAbstractWideMapWrapper<K, Counter>
{

    private static Logger log = LoggerFactory.getLogger(ThriftCounterWideMapWrapper.class);

    private Object id;
    private ThriftGenericWideRowDao wideMapCounterDao;
    private PropertyMeta<K, Counter> propertyMeta;

    @Override
    public Counter get(K key)
    {
        log.trace("Get counter value having key {}", key);
        Validator.validateNotNull(key, "Key should be provided to insert data into WideMap");

        final Composite comp = thriftCompositeFactory.createForQuery(propertyMeta, key, EQUAL);

        Long counterValue = context.executeWithReadConsistencyLevel(
                new SafeExecutionContext<Long>()
                {
                    @Override
                    public Long execute()
                    {
                        return wideMapCounterDao.getCounterValue(id, comp);
                    }
                }, propertyMeta.getReadConsistencyLevel());

        if (counterValue != null)
        {
            return ThriftCounterWrapperBuilder.builder(context) //
                    .columnName(comp)
                    .counterDao(wideMapCounterDao)
                    .key(id)
                    .readLevel(propertyMeta.getReadConsistencyLevel())
                    .writeLevel(propertyMeta.getWriteConsistencyLevel())
                    .build();
        }
        else
        {
            return null;
        }

    }

    @Override
    public void insert(K key, Counter value, int ttl)
    {
        throw new UnsupportedOperationException("Cannot insert counter value with ttl");
    }

    @Override
    public void insert(K key, final Counter value)
    {
        log.trace("Insert counter value {} with key {}", value, key);
        Validator.validateNotNull(key, "Key should be provided to insert data into WideMap");
        Validator.validateNotNull(value, "Value should be provided to insert data into WideMap");

        final Composite comp = thriftCompositeFactory.createBaseComposite(propertyMeta, key);

        context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
        {
            @Override
            public Void execute()
            {
                wideMapCounterDao.incrementCounter(id, comp, value.get());
                return null;
            }
        }, propertyMeta.getWriteConsistencyLevel());
    }

    @Override
    public List<KeyValue<K, Counter>> find(K start, K end, final int count, BoundingMode bounds,
            final OrderingMode ordering)
    {
        queryValidator.validateBoundsForQuery(propertyMeta, start, end, ordering);

        final Composite[] queryComps = thriftCompositeFactory.createForQuery(propertyMeta, start,
                end, bounds, ordering);
        if (log.isTraceEnabled())
        {
            log.trace("Find key/value pairs in range {} / {} with bounding {} and ordering {}",
                    format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
        }

        List<HCounterColumn<Composite>> hColumns = context.executeWithReadConsistencyLevel(
                new SafeExecutionContext<List<HCounterColumn<Composite>>>()
                {
                    @Override
                    public List<HCounterColumn<Composite>> execute()
                    {
                        return wideMapCounterDao.findCounterColumnsRange(id, queryComps[0],
                                queryComps[1], count, ordering.isReverse());
                    }
                }, propertyMeta.getReadConsistencyLevel());

        return thriftKeyValueFactory.createCounterKeyValueList(context, propertyMeta, hColumns);
    }

    @Override
    public List<Counter> findValues(K start, K end, final int count, BoundingMode bounds,
            final OrderingMode ordering)
    {
        queryValidator.validateBoundsForQuery(propertyMeta, start, end, ordering);

        final Composite[] queryComps = thriftCompositeFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);

        if (log.isTraceEnabled())
        {
            log.trace("Find value in range {} / {} with bounding {} and ordering {}",
                    format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
        }

        List<HCounterColumn<Composite>> hColumns = context.executeWithReadConsistencyLevel(
                new SafeExecutionContext<List<HCounterColumn<Composite>>>()
                {
                    @Override
                    public List<HCounterColumn<Composite>> execute()
                    {
                        return wideMapCounterDao.findCounterColumnsRange(id, queryComps[0],
                                queryComps[1], count, ordering.isReverse());
                    }
                }, propertyMeta.getReadConsistencyLevel());

        return thriftKeyValueFactory.createCounterValueList(context, propertyMeta, hColumns);
    }

    @Override
    public List<K> findKeys(K start, K end, final int count, BoundingMode bounds,
            final OrderingMode ordering)
    {
        queryValidator.validateBoundsForQuery(propertyMeta, start, end, ordering);
        final Composite[] queryComps = thriftCompositeFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);

        if (log.isTraceEnabled())
        {
            log.trace("Find keys in range {} / {} with bounding {} and ordering {}",
                    format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
        }

        List<HCounterColumn<Composite>> hColumns = context.executeWithReadConsistencyLevel(
                new SafeExecutionContext<List<HCounterColumn<Composite>>>()
                {
                    @Override
                    public List<HCounterColumn<Composite>> execute()
                    {
                        return wideMapCounterDao.findCounterColumnsRange(id, queryComps[0],
                                queryComps[1], count, ordering.isReverse());
                    }
                }, propertyMeta.getReadConsistencyLevel());

        return thriftKeyValueFactory.createCounterKeyList(propertyMeta, hColumns);
    }

    @Override
    public KeyValueIterator<K, Counter> iterator(K start, K end, final int count,
            BoundingMode bounds, final OrderingMode ordering)
    {
        final Composite[] queryComps = thriftCompositeFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);

        if (log.isTraceEnabled())
        {
            log
                    .trace("Iterate in range {} / {} with bounding {} and ordering {} and batch of {} elements",
                            format(queryComps[0]), format(queryComps[1]), bounds.name(),
                            ordering.name(), count);
        }

        ThriftCounterSliceIterator<?> columnSliceIterator = context
                .executeWithReadConsistencyLevel(
                        new SafeExecutionContext<ThriftCounterSliceIterator<?>>()
                        {
                            @Override
                            public ThriftCounterSliceIterator<?> execute()
                            {
                                return wideMapCounterDao.getCounterColumnsIterator(id,
                                        queryComps[0], queryComps[1], ordering.isReverse(), count);
                            }
                        }, propertyMeta.getReadConsistencyLevel());

        return thriftIteratorFactory.createCounterKeyValueIterator(context, columnSliceIterator,
                propertyMeta);
    }

    @Override
    public void remove(K key)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void remove(K start, K end, BoundingMode bounds)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void removeFirst(int count)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void removeLast(int count)
    {

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

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public void insert(K key, Counter value, int ttl, ConsistencyLevel writeLevel)
    {

        throw new UnsupportedOperationException("Cannot insert counter value with ttl");
    }

    @Override
    public List<KeyValue<K, Counter>> find(K start, K end, int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<KeyValue<K, Counter>> find(K start, K end, int count, BoundingMode bounds,
            OrderingMode ordering, ConsistencyLevel readLevel)
    {
        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<KeyValue<K, Counter>> findBoundsExclusive(K start, K end, int count,
            ConsistencyLevel readLevel)
    {
        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<KeyValue<K, Counter>> findReverse(K start, K end, int count,
            ConsistencyLevel readLevel)
    {
        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<KeyValue<K, Counter>> findReverseBoundsExclusive(K start, K end, int count,
            ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValue<K, Counter> findFirst(ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<KeyValue<K, Counter>> findFirst(int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValue<K, Counter> findLast(ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<KeyValue<K, Counter>> findLast(int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<Counter> findValues(K start, K end, int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<Counter> findValues(K start, K end, int count, BoundingMode bounds,
            OrderingMode ordering, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<Counter> findBoundsExclusiveValues(K start, K end, int count,
            ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<Counter> findReverseValues(K start, K end, int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<Counter> findReverseBoundsExclusiveValues(K start, K end, int count,
            ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public Counter findFirstValue(ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<Counter> findFirstValues(int count, ConsistencyLevel readLevel)
    {

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

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<K> findBoundsExclusiveKeys(K start, K end, int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<K> findReverseKeys(K start, K end, int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<K> findReverseBoundsExclusiveKeys(K start, K end, int count,
            ConsistencyLevel readLevel)
    {

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

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public K findLastKey(ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public List<K> findLastKeys(int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValueIterator<K, Counter> iterator(ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValueIterator<K, Counter> iterator(K start, K end, int count, BoundingMode bounds,
            OrderingMode ordering, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValueIterator<K, Counter> iterator(int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValueIterator<K, Counter> iterator(K start, K end, int count,
            ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValueIterator<K, Counter> iteratorBoundsExclusive(K start, K end, int count,
            ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValueIterator<K, Counter> iteratorReverse(ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValueIterator<K, Counter> iteratorReverse(int count, ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValueIterator<K, Counter> iteratorReverse(K start, K end, int count,
            ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public KeyValueIterator<K, Counter> iteratorReverseBoundsExclusive(K start, K end, int count,
            ConsistencyLevel readLevel)
    {

        throw new UnsupportedOperationException(
                "Please set runtime consistency level at Counter level instead of at WideMap level");
    }

    @Override
    public void remove(K key, ConsistencyLevel writeLevel)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void remove(K start, K end, ConsistencyLevel writeLevel)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void remove(K start, K end, BoundingMode bounds, ConsistencyLevel writeLevel)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void removeBoundsExclusive(K start, K end, ConsistencyLevel writeLevel)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void removeFirst(ConsistencyLevel writeLevel)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void removeFirst(int count, ConsistencyLevel writeLevel)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void removeLast(ConsistencyLevel writeLevel)
    {

        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    @Override
    public void removeLast(int count, ConsistencyLevel writeLevel)
    {
        throw new UnsupportedOperationException("Cannot remove counter value");
    }

    public void setId(Object id)
    {
        this.id = id;
    }

    public void setPropertyMeta(PropertyMeta<K, Counter> propertyMeta)
    {
        this.propertyMeta = propertyMeta;
    }

    public void setWideMapCounterDao(ThriftGenericWideRowDao wideMapCounterDao)
    {
        this.wideMapCounterDao = wideMapCounterDao;
    }
}
