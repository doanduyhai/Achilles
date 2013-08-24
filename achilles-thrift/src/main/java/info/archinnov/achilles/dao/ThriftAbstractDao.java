package info.archinnov.achilles.dao;

import static info.archinnov.achilles.logger.ThriftLoggerHelper.format;
import static me.prettyprint.hector.api.factory.HFactory.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.ThriftCounterSliceIterator;
import info.archinnov.achilles.iterator.ThriftJoinSliceIterator;
import info.archinnov.achilles.iterator.ThriftSliceIterator;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import info.archinnov.achilles.serializer.ThriftSerializerUtils;
import info.archinnov.achilles.validation.Validator;
import java.util.List;
import me.prettyprint.cassandra.model.HCounterColumnImpl;
import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
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
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

/**
 * ThriftAbstractDao
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class ThriftAbstractDao
{
    public static final String LOGGER_NAME = "ACHILLES_DAO";
    private static final Logger log = LoggerFactory.getLogger(LOGGER_NAME);

    protected Keyspace keyspace;
    protected Cluster cluster;
    protected Serializer<Composite> columnNameSerializer;
    protected String columnFamily;
    protected AchillesConsistencyLevelPolicy policy;
    protected Pair<?, ?> rowkeyAndValueClasses;

    public static int DEFAULT_LENGTH = 100;

    protected ThriftAbstractDao() {
    }

    protected ThriftAbstractDao(Cluster cluster, Keyspace keyspace, String cf,
            AchillesConsistencyLevelPolicy policy,
            Pair<?, ?> rowkeyAndValueClasses)
    {
        Validator.validateNotNull(cluster, "Cluster should not be null");
        Validator.validateNotNull(keyspace, "keyspace should not be null");
        Validator.validateNotNull(keyspace, "policy should not be null");
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.columnFamily = cf;
        this.policy = policy;
        this.rowkeyAndValueClasses = rowkeyAndValueClasses;
    }

    private <T> T reinitConsistencyLevels(SafeExecutionContext<T> context)
    {
        log.trace("Execute safely and reinit consistency level in thread {}",
                Thread.currentThread());
        try
        {
            return context.execute();
        } finally
        {
            this.policy.reinitDefaultConsistencyLevels();
        }
    }

    protected <V> Function<HColumn<Composite, V>, V> getHColumnToValueFn()
    {
        return new Function<HColumn<Composite, V>, V>()

        {
            @Override
            public V apply(HColumn<Composite, V> hColumn)
            {
                return hColumn.getValue();
            }
        };
    }

    private <V> Function<HColumn<Composite, V>, Pair<Composite, V>> getHColumnToPairFn()
    {
        return new Function<HColumn<Composite, V>, Pair<Composite, V>>()
        {
            @Override
            public Pair<Composite, V> apply(HColumn<Composite, V> hColumn)
            {
                return Pair.create(hColumn.getName(), hColumn.getValue());
            }
        };
    }

    public <K, V> void insertColumnBatch(K key, Composite name, V value, Optional<Integer> ttlO,
            Optional<Long> timestampO, Mutator<K> mutator)
    {
        if (log.isTraceEnabled())
            log.trace("Insert column {} into column family {} with key {}", format(name),
                    columnFamily, key);

        HColumn<Composite, V> column;
        if (ttlO.isPresent() && timestampO.isPresent())
        {
            column = HFactory.createColumn(name, value, timestampO.get(), ttlO.get(), columnNameSerializer,
                    this.<V> valSrz());
        }
        else if (ttlO.isPresent())
        {
            column = HFactory.createColumn(name, value, ttlO.get(), columnNameSerializer, this.<V> valSrz());
        }
        else if (timestampO.isPresent())
        {
            column = HFactory.createColumn(name, value, timestampO.get(), columnNameSerializer, this.<V> valSrz());
        }
        else
        {
            column = HFactory.createColumn(name, value, columnNameSerializer, this.<V> valSrz());
        }
        mutator.addInsertion(key, columnFamily, column);
    }

    public <K, V> V getValue(final K key, final Composite name)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Get value from column family {} with key {} and column name {}",
                    columnFamily, key,
                    format(name));
        }

        V result = null;
        HColumn<Composite, V> column = getColumn(key, name);
        if (column != null)
        {
            result = column.getValue();
        }
        return result;
    }

    public <K, V> HColumn<Composite, V> getColumn(final K key, final Composite name)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Get column from column family {} with key {} and column name {}",
                    columnFamily, key,
                    format(name));
        }

        this.policy.loadConsistencyLevelForRead(columnFamily);
        return reinitConsistencyLevels(new SafeExecutionContext<HColumn<Composite, V>>()
        {
            @Override
            public HColumn<Composite, V> execute()
            {
                return HFactory
                        .createColumnQuery(keyspace, ThriftAbstractDao.this.<K> rowSrz(),
                                columnNameSerializer,
                                ThriftAbstractDao.this.<V> valSrz())
                        .setColumnFamily(columnFamily)
                        .setKey(key)
                        .setName(name)
                        .execute()
                        .get();
            }
        });
    }

    public <K, V> void setValue(K key, Composite name, V value)
    {
        log.trace("Set value {} to column family {} with key {} , column name {}", value,
                columnFamily, key, name);

        Mutator<K> mutator = HFactory.createMutator(keyspace, this.<K> rowSrz());
        this.setValueBatch(key, name, value, Optional.<Integer> absent(), Optional.<Long> absent(), mutator);
        this.executeMutator(mutator);
    }

    public <K, V> void setValueBatch(K key, Composite name, V value, Optional<Integer> ttlO,
            Optional<Long> timestampO, Mutator<K> mutator)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace("Set value {} as batch mutation to column family {} with key {} , column name {} and ttl {}",
                            value, columnFamily, key, format(name), ttlO);
        }
        HColumn<Composite, V> column;
        if (ttlO.isPresent() && timestampO.isPresent())
        {
            column = HFactory.createColumn(name, value, timestampO.get(), ttlO.get(), columnNameSerializer,
                    this.<V> valSrz());
        }
        else if (ttlO.isPresent())
        {
            column = HFactory.createColumn(name, value, ttlO.get(), columnNameSerializer, this.<V> valSrz());
        }
        else if (timestampO.isPresent())
        {
            column = HFactory.createColumn(name, value, timestampO.get(), columnNameSerializer, this.<V> valSrz());
        }
        else
        {
            column = HFactory.createColumn(name, value, columnNameSerializer, this.<V> valSrz());
        }
        mutator.addInsertion(key, columnFamily, column);
    }

    public <K> void removeColumnBatch(K key, Composite name, Mutator<K> mutator)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Remove column name {} as batch mutation from column family {} with key {} ",
                    format(name),
                    columnFamily, key);
        }
        mutator.addDeletion(key, columnFamily, name, columnNameSerializer);
    }

    public <K, V> void removeColumnRangeBatch(K key, Composite start, Composite end,
            Mutator<K> mutator)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Remove column slice within range having inclusive start/end {}/{} column names as batch mutation from column family {} with key {} ",
                            format(start), format(end), columnFamily, key);
        }
        this.removeColumnRangeBatch(key, start, end, false, Integer.MAX_VALUE, mutator);
    }

    public <K, V> void removeColumnRangeBatch(K key, Composite start, Composite end,
            boolean reverse, int count,
            Mutator<K> mutator)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Remove {} columns slice within range having inclusive start/end {}/{} column names as batch mutation from column family {} with key {} and reserver {}",
                            count, format(start), format(end), columnFamily, key, reverse);
        }
        List<HColumn<Composite, V>> columns = createSliceQuery(keyspace, this.<K> rowSrz(),
                columnNameSerializer,
                this.<V> valSrz())
                .setColumnFamily(columnFamily)
                .setKey(key)
                .setRange(start, end, reverse, count)
                .execute()
                .get()
                .getColumns();

        for (HColumn<Composite, V> column : columns)
        {
            mutator.addDeletion(key, columnFamily, column.getName(), columnNameSerializer);
        }
    }

    public <K, V> List<V> findValuesRange(final K key, final Composite start, final Composite end,
            final boolean reverse, final int count)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Find {} values slice within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {}",
                            count, format(start), format(end), columnFamily, key, reverse);
        }
        this.policy.loadConsistencyLevelForRead(columnFamily);
        List<HColumn<Composite, V>> columns = reinitConsistencyLevels(new SafeExecutionContext<List<HColumn<Composite, V>>>()
        {
            @Override
            public List<HColumn<Composite, V>> execute()
            {
                return createSliceQuery(keyspace, ThriftAbstractDao.this.<K> rowSrz(),
                        columnNameSerializer,
                        ThriftAbstractDao.this.<V> valSrz())
                        .setColumnFamily(columnFamily)
                        .setKey(key)
                        .setRange(start, end, reverse, count)
                        .execute()
                        .get()
                        .getColumns();
            }
        });
        return Lists.transform(columns, this.<V> getHColumnToValueFn());
    }

    public <K, V> List<Pair<Composite, V>> findColumnsRange(final K key, final Composite start,
            final Composite end,
            final boolean reverse, final int count)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Find {} columns slice within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {}",
                            count, format(start), format(end), columnFamily, key, reverse);
        }
        this.policy.loadConsistencyLevelForRead(columnFamily);
        List<HColumn<Composite, V>> columns = reinitConsistencyLevels(new SafeExecutionContext<List<HColumn<Composite, V>>>()
        {
            @Override
            public List<HColumn<Composite, V>> execute()
            {
                return createSliceQuery(keyspace, ThriftAbstractDao.this.<K> rowSrz(),
                        columnNameSerializer,
                        ThriftAbstractDao.this.<V> valSrz())
                        .setColumnFamily(columnFamily)
                        .setKey(key)
                        .setRange(start, end, reverse, count)
                        .execute()
                        .get()
                        .getColumns();
            }
        });
        return Lists.transform(columns, this.<V> getHColumnToPairFn());
    }

    public <K, V> List<HColumn<Composite, V>> findRawColumnsRange(final K key,
            final Composite start,
            final Composite end, final int count, final boolean reverse)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Find raw {} columns slice within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {}",
                            count, format(start), format(end), columnFamily, key, reverse);
        }

        this.policy.loadConsistencyLevelForRead(columnFamily);
        return reinitConsistencyLevels(new SafeExecutionContext<List<HColumn<Composite, V>>>()
        {
            @Override
            public List<HColumn<Composite, V>> execute()
            {
                List<HColumn<Composite, V>> columns = createSliceQuery(keyspace, ThriftAbstractDao.this.<K> rowSrz(),
                        columnNameSerializer,
                        ThriftAbstractDao.this.<V> valSrz())
                        .setColumnFamily(columnFamily)
                        .setKey(key)
                        .setRange(start, end, reverse, count)
                        .execute()
                        .get()
                        .getColumns();

                return columns;
            }
        });
    }

    public <K, V> List<HCounterColumn<Composite>> findCounterColumnsRange(final K key,
            final Composite start,
            final Composite end, final int count, final boolean reverse)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
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
                        .createCounterSliceQuery(keyspace, ThriftAbstractDao.this.<K> rowSrz(),
                                columnNameSerializer)
                        .setColumnFamily(columnFamily)
                        .setKey(key)
                        .setRange(start, end, reverse, count)
                        .execute()
                        .get()
                        .getColumns();
            }
        });
    }

    public <K, V> ThriftSliceIterator<K, V> getColumnsIterator(K key, Composite start,
            Composite end,
            boolean reverse, int length)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Get columns slice iterator within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {} by batch of {} elements",
                            format(start), format(end), columnFamily, key, reverse, length);
        }

        SliceQuery<K, Composite, V> query = createSliceQuery(keyspace,
                ThriftAbstractDao.this.<K> rowSrz(),
                columnNameSerializer, ThriftAbstractDao.this.<V> valSrz()).setColumnFamily(
                columnFamily).setKey(key);

        return new ThriftSliceIterator<K, V>(policy, columnFamily, query, start, end, reverse,
                length);
    }

    public <K, V> ThriftCounterSliceIterator<K> getCounterColumnsIterator(K key, Composite start,
            Composite end,
            boolean reverse, int length)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Get counter columns slice iterator within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {} by batch of {} elements",
                            format(start), format(end), columnFamily, key, reverse, length);
        }

        this.policy.loadConsistencyLevelForRead(columnFamily);
        SliceCounterQuery<K, Composite> query = createCounterSliceQuery(keyspace,
                this.<K> rowSrz(),
                columnNameSerializer).setColumnFamily(columnFamily).setKey(key);

        return new ThriftCounterSliceIterator<K>(policy, columnFamily, query, start, end, reverse,
                length);
    }

    public <K, KEY, VALUE> ThriftJoinSliceIterator<K, KEY, VALUE> getJoinColumnsIterator(
            ThriftGenericEntityDao joinEntityDao, PropertyMeta propertyMeta, K key,
            Composite start,
            Composite end, boolean reversed, int count)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Get join columns iterator within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {} by batch of {} elements; for property {}",
                            format(start), format(end), columnFamily, key, reversed, count,
                            propertyMeta.getPropertyName());
        }

        SliceQuery<K, Composite, Object> query = createSliceQuery(keyspace, this.<K> rowSrz(),
                columnNameSerializer,
                this.<Object> valSrz()).setColumnFamily(columnFamily).setKey(key);

        return new ThriftJoinSliceIterator<K, KEY, VALUE>(policy, joinEntityDao, columnFamily,
                propertyMeta, query,
                start, end, reversed, count);
    }

    public <K, V> Rows<K, Composite, V> multiGetSliceRange(final List<K> keys,
            final Composite start,
            final Composite end, final boolean reverse, final int size)
    {
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Multi get columns slice within range having inclusive start/end {}/{} column names from column family {} with key {} and reverse {} by batch of {} elements; for property {}",
                            format(start), format(end), columnFamily, StringUtils.join(keys, ","),
                            reverse, size);
        }

        this.policy.loadConsistencyLevelForRead(columnFamily);
        return reinitConsistencyLevels(new SafeExecutionContext<Rows<K, Composite, V>>()
        {
            @Override
            public Rows<K, Composite, V> execute()
            {
                return HFactory
                        .createMultigetSliceQuery(keyspace, ThriftAbstractDao.this.<K> rowSrz(),
                                columnNameSerializer, ThriftAbstractDao.this.<V> valSrz())
                        .setColumnFamily(columnFamily)
                        .setKeys(keys)
                        .setRange(start, end, reverse, size)
                        .execute()
                        .get();
            }
        });
    }

    public <K> void removeRowBatch(K key, Mutator<K> mutator)
    {
        log.trace("Remove row as batch mutation from column family {} with key {}", columnFamily,
                key);

        mutator.addDeletion(key, columnFamily);
    }

    public <K> void incrementCounter(K key, Composite name, Long value)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Incrementing counter column {} with key {} from column family {} by {}",
                    format(name), key,
                    columnFamily, value);
        }
        Mutator<K> mutator = buildMutator();
        mutator.addCounter(key, columnFamily, new HCounterColumnImpl<Composite>(name, value,
                ThriftSerializerUtils.COMPOSITE_SRZ));
        executeMutator(mutator);
    }

    public <K> void decrementCounter(K key, Composite name, Long value)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Decrementing counter column {} with key {} from column family {} by {}",
                    format(name), key,
                    columnFamily, value);
        }
        Mutator<K> mutator = buildMutator();
        mutator.addCounter(key, columnFamily, new HCounterColumnImpl<Composite>(name, value * -1L,
                ThriftSerializerUtils.COMPOSITE_SRZ));
        executeMutator(mutator);
    }

    public <K> Long getCounterValue(K key, Composite name)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Get counter value column {} with key {} from column family {}",
                    format(name), key,
                    columnFamily);
        }

        Long counterValue = null;
        HCounterColumn<Composite> counterColumn = getCounterColumn(key, name);
        if (counterColumn != null)
        {
            counterValue = counterColumn.getValue();
        }

        return counterValue;
    }

    public <K> HCounterColumn<Composite> getCounterColumn(K key, Composite name)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Get counter  column {} with key {} from column family {}", format(name),
                    key, columnFamily);
        }

        final CounterQuery<K, Composite> counter = new ThriftCounterColumnQuery<K, Composite>(
                keyspace,
                this.<K> rowSrz(), columnNameSerializer)
                .setColumnFamily(columnFamily)
                .setKey(key)
                .setName(name);

        this.policy.loadConsistencyLevelForRead(columnFamily);
        return reinitConsistencyLevels(new SafeExecutionContext<HCounterColumn<Composite>>()
        {
            @Override
            public HCounterColumn<Composite> execute()
            {
                return counter.execute().get();
            }
        });
    }

    public <K> void removeCounterBatch(K key, Composite name, Mutator<K> mutator)
    {
        if (log.isTraceEnabled())
        {
            log.trace(
                    "Remove counter column {} as batch mutation with key {} from column family {}",
                    format(name),
                    key, columnFamily);
        }

        mutator.deleteCounter(key, columnFamily, name, columnNameSerializer);
    }

    public <K> void removeCounterRowBatch(K key, Mutator<K> mutator)
    {
        log.trace("Remove counter row as batch mutation with key {} from column family {}", key,
                columnFamily);

        SliceCounterQuery<K, Composite> query = HFactory
                .createCounterSliceQuery(keyspace, this.<K> rowSrz(), columnNameSerializer)
                .setColumnFamily(columnFamily).setKey(key);

        ThriftCounterSliceIterator<K> iterator = new ThriftCounterSliceIterator<K>(policy,
                columnFamily, query,
                (Composite) null, (Composite) null, false, DEFAULT_LENGTH);

        while (iterator.hasNext())
        {
            HCounterColumn<Composite> counterCol = iterator.next();
            mutator.deleteCounter(key, columnFamily, counterCol.getName(), columnNameSerializer);
        }
    }

    public <K> void truncate()
    {
        cluster.truncate(keyspace.getKeyspaceName(), columnFamily);
    }

    public void truncateCounters()
    {
        cluster.truncate(keyspace.getKeyspaceName(), AchillesCounter.THRIFT_COUNTER_CF);
    }

    public <K> Mutator<K> buildMutator()
    {
        return HFactory.createMutator(this.keyspace, this.<K> rowSrz());
    }

    public <K> void executeMutator(final Mutator<K> mutator)
    {
        log.trace("Execute safely mutator with {} mutations for column family {}",
                mutator.getPendingMutationCount(),
                columnFamily);

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

    protected <T> Serializer<T> rowSrz()
    {
        return ThriftSerializerTypeInferer.<T> getSerializer((Class<?>) rowkeyAndValueClasses.left);
    }

    protected <T> Serializer<T> valSrz()
    {
        return ThriftSerializerTypeInferer
                .<T> getSerializer((Class<?>) rowkeyAndValueClasses.right);
    }
}
