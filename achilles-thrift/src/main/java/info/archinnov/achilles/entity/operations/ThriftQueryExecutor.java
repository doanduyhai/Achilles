package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.clustered.ClusteredEntityFactory;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.ThriftQueryExecutorImpl;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.iterator.ThriftClusteredEntityIterator;
import info.archinnov.achilles.iterator.ThriftCounterClusteredEntityIterator;
import info.archinnov.achilles.iterator.ThriftCounterSliceIterator;
import info.archinnov.achilles.iterator.ThriftJoinSliceIterator;
import info.archinnov.achilles.iterator.ThriftSliceIterator;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ThriftQueryExecutor implements QueryExecutor
{

    private static final Optional<ConsistencyLevel> NO_CONSISTENCY_LEVEL = Optional
            .<ConsistencyLevel> absent();
    private static final Optional<Integer> NO_TTL = Optional.<Integer> absent();

    private ThriftDaoContext daoContext;
    private AchillesConsistencyLevelPolicy consistencyPolicy;
    private ConfigurationContext configContext;

    private ClusteredEntityFactory factory = new ClusteredEntityFactory();
    private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
    private ReflectionInvoker invoker = new ReflectionInvoker();
    private ThriftQueryExecutorImpl executorImpl = new ThriftQueryExecutorImpl();

    public ThriftQueryExecutor(ConfigurationContext configContext, ThriftDaoContext daoContext,
            AchillesConsistencyLevelPolicy consistencyPolicy)
    {
        this.configContext = configContext;
        this.daoContext = daoContext;
        this.consistencyPolicy = consistencyPolicy;
    }

    @Override
    public <T> List<T> get(final SliceQuery<T> query)
    {
        ThriftPersistenceContext context = buildContext(query);
        EntityMeta meta = query.getMeta();
        PropertyMeta<?, ?> pm = meta.getFirstMeta();

        List<T> clusteredEntities = null;
        switch (pm.type())
        {
            case JOIN_SIMPLE:
            case SIMPLE:
                List<HColumn<Composite, Object>> hColumns = executorImpl
                        .findColumns(query, context);
                clusteredEntities = factory.buildClusteredEntities(query.getEntityClass(), context,
                        hColumns);
                break;
            case COUNTER:
                List<HCounterColumn<Composite>> hCounterColumns = executorImpl.findCounterColumns(
                        query, context);
                clusteredEntities = factory.buildCounterClusteredEntities(query.getEntityClass(),
                        context, hCounterColumns);
                break;
            default:
                throw new AchillesException("Cannot get entities for clustered value of type '"
                        + pm.type().name() + "' and clustered entity class '"
                        + query.getEntityClass().getCanonicalName() + "'");
        }

        return Lists.transform(clusteredEntities,
                this.<T> getProxyTransformer(context, pm.getGetter()));
    }

    @Override
    public <T> Iterator<T> iterator(final SliceQuery<T> query)
    {
        ThriftPersistenceContext context = buildContext(query);
        EntityMeta meta = query.getMeta();
        PropertyMeta<?, ?> pm = meta.getFirstMeta();
        Class<T> entityClass = query.getEntityClass();
        switch (pm.type())
        {
            case SIMPLE:
                ThriftSliceIterator<Object, Object> columnsIterator = executorImpl
                        .getColumnsIterator(query, context);
                return new ThriftClusteredEntityIterator<T>(entityClass,
                        columnsIterator, context);

            case JOIN_SIMPLE:
                ThriftJoinSliceIterator<Object, Object, Object> joinColumnsIterator = executorImpl
                        .getJoinColumnsIterator(query, context);
                return new ThriftClusteredEntityIterator<T>(entityClass,
                        joinColumnsIterator, context);
            case COUNTER:
                ThriftCounterSliceIterator<Object> counterColumnsIterator = executorImpl
                        .getCounterColumnsIterator(query, context);
                return new ThriftCounterClusteredEntityIterator<T>(entityClass,
                        counterColumnsIterator, context);
            default:
                throw new AchillesException("Cannot get iterator for clustered value of type '"
                        + pm.type().name() + "' and clustered entity class '"
                        + entityClass.getCanonicalName() + "'");
        }
    }

    @Override
    public <T> void remove(final SliceQuery<T> query)
    {
        ThriftPersistenceContext context = buildContext(query);
        EntityMeta meta = query.getMeta();
        PropertyMeta<?, ?> pm = meta.getFirstMeta();

        switch (pm.type())
        {
            case JOIN_SIMPLE:
            case SIMPLE:
                List<HColumn<Composite, Object>> hColumns = executorImpl
                        .findColumns(query, context);
                executorImpl.removeColumns(hColumns, query.getConsistencyLevel(), context);
                break;
            case COUNTER:
                List<HCounterColumn<Composite>> hCounterColumns = executorImpl.findCounterColumns(
                        query, context);
                executorImpl.removeCounterColumns(hCounterColumns, query.getConsistencyLevel(),
                        context);
                break;
            default:
                throw new AchillesException("Cannot remove clustered value of type '"
                        + pm.type().name() + "' and clustered entity class '"
                        + query.getEntityClass().getCanonicalName() + "'");
        }
    }

    private <T> ThriftPersistenceContext buildContext(final SliceQuery<T> query)
    {
        ThriftImmediateFlushContext flushContext = new ThriftImmediateFlushContext(daoContext,
                consistencyPolicy, Optional.fromNullable(query.getConsistencyLevel()),
                NO_CONSISTENCY_LEVEL, NO_TTL);

        Object partitionKey = query.getPartitionKey();
        EntityMeta meta = query.getMeta();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();

        Object embeddedId = invoker.instanciateEmbeddedIdWithPartitionKey(idMeta, partitionKey);
        return new ThriftPersistenceContext(meta, configContext,
                daoContext, flushContext, query.getEntityClass(), embeddedId, new HashSet<String>());
    }

    private <T> Function<T, T> getProxyTransformer(final ThriftPersistenceContext context,
            final Method getter)
    {
        return new Function<T, T>()
        {
            @Override
            public T apply(T clusteredEntity)
            {
                Object embeddedId = invoker.getPrimaryKey(clusteredEntity, context.getIdMeta());
                ThriftPersistenceContext duplicate = context.duplicateWithPrimaryKey(embeddedId);
                return proxifier.buildProxy(clusteredEntity, duplicate, Sets.newHashSet(getter));
            }
        };
    }
}
