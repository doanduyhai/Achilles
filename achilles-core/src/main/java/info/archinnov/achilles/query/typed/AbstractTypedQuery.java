package info.archinnov.achilles.query.typed;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.EntitiesWithExecutionInfo;
import info.archinnov.achilles.internal.async.RowsWithExecutionInfo;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityMapper;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.statement.wrapper.NativeStatementWrapper;
import info.archinnov.achilles.iterator.AchillesIterator;
import info.archinnov.achilles.listener.LWTResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ITERATOR;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROW;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS_WITH_EXECUTION_INFO;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState.MANAGED;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState.NOT_MANAGED;

public abstract class AbstractTypedQuery<T> {

    private static final Logger log = LoggerFactory.getLogger(AbstractTypedQuery.class);
    protected static final FutureCallback<Object>[] NO_CALLBACKS = new FutureCallback[]{};

    protected final NativeStatementWrapper nativeStatementWrapper;

    protected DaoContext daoContext;
    protected ExecutorService executorService;
    protected Map<String, PropertyMeta> propertiesMap;
    protected EntityMeta meta;
    protected PersistenceContextFactory contextFactory;
    protected EntityMeta.EntityState entityState;
    protected Object[] boundValues;
    protected Optional<PagingState> pagingStateO = Optional.absent();

    protected EntityMapper mapper = EntityMapper.Singleton.INSTANCE.get();
    protected EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();
    protected AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();

    public AbstractTypedQuery(Class<T> entityClass, DaoContext daoContext, ConfigurationContext configContext, Statement statement, EntityMeta meta,
                      PersistenceContextFactory contextFactory, EntityMeta.EntityState entityState, Object[] boundValues) {
        this.daoContext = daoContext;
        this.executorService = configContext.getExecutorService();
        this.boundValues = boundValues;
        this.nativeStatementWrapper = new NativeStatementWrapper(entityClass, statement, this.boundValues, Optional.<LWTResultListener>absent());
        this.meta = meta;
        this.contextFactory = contextFactory;
        this.entityState = entityState;
        this.propertiesMap = transformPropertiesMap(meta);
    }

    protected ListenableFuture<EntitiesWithExecutionInfo<T>> asyncGetInternal(FutureCallback<Object>... asyncListeners) {
        log.debug("Get results asynchronously for typed query '{}'", nativeStatementWrapper.getStatement());

        if (pagingStateO.isPresent()) {
            nativeStatementWrapper.setPagingState(pagingStateO.get());
        }

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);
        final ListenableFuture<RowsWithExecutionInfo> futureRows = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROWS_WITH_EXECUTION_INFO);

        Function<RowsWithExecutionInfo, EntitiesWithExecutionInfo<T>> rowsToEntities = rowsToEntities();
        Function<EntitiesWithExecutionInfo<T>, EntitiesWithExecutionInfo<T>> applyTriggers = applyTriggersToEntities();
        Function<EntitiesWithExecutionInfo<T>, EntitiesWithExecutionInfo<T>> maybeCreateProxy = proxifyEntities();

        final ListenableFuture<EntitiesWithExecutionInfo<T>> rawEntities = asyncUtils.transformFuture(futureRows, rowsToEntities);
        final ListenableFuture<EntitiesWithExecutionInfo<T>> entitiesWithTriggers = asyncUtils.transformFuture(rawEntities, applyTriggers);

        asyncUtils.maybeAddAsyncListeners(entitiesWithTriggers, asyncListeners);

        final ListenableFuture<EntitiesWithExecutionInfo<T>> maybeProxyCreated = asyncUtils.transformFuture(entitiesWithTriggers, maybeCreateProxy);

        return maybeProxyCreated;
    }

    protected AchillesFuture<T> asyncGetFirstInternal(FutureCallback<Object>... asyncListeners) {
        log.debug("Get first result asynchronously for typed query '{}'", nativeStatementWrapper.getStatement());

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);
        final ListenableFuture<Row> futureRow = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROW, executorService);

        Function<Row, T> rowToEntity = rowToEntity();
        Function<T, T> applyTriggers = applyTriggersToEntity();
        Function<T, T> maybeCreateProxy = proxifyEntity();

        final ListenableFuture<T> rawEntity = asyncUtils.transformFuture(futureRow, rowToEntity);
        final ListenableFuture<T> entityWithTriggers = asyncUtils.transformFuture(rawEntity, applyTriggers);

        asyncUtils.maybeAddAsyncListeners(entityWithTriggers, asyncListeners);

        final ListenableFuture<T> maybeProxyCreated = asyncUtils.transformFuture(entityWithTriggers, maybeCreateProxy);

        return asyncUtils.buildInterruptible(maybeProxyCreated);
    }

    protected AchillesFuture<Iterator<T>> asyncIteratorInternal(Optional<Integer> fetchSizeO, FutureCallback<Object>... asyncListeners) {
        final Statement statement = nativeStatementWrapper.getStatement();
        log.debug("Get iterator asynchronously for typed query '{}'", statement.toString());

        if (fetchSizeO.isPresent()) {
            statement.setFetchSize(fetchSizeO.get());
        }
        final PersistenceContext persistenceContext = contextFactory.newContextForTypedQuery(meta.getEntityClass());

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);
        final ListenableFuture<Iterator<Row>> futureIterator = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ITERATOR, executorService);

        Function<Iterator<Row>, Iterator<T>> rowToIterator = new Function<Iterator<Row>, Iterator<T>>() {
            @Override
            public Iterator<T> apply(Iterator<Row> rowIterator) {
                return new AchillesIterator<>(meta, persistenceContext, rowIterator);
            }
        };
        final ListenableFuture<Iterator<T>> listenableFuture = asyncUtils.transformFuture(futureIterator, rowToIterator);
        asyncUtils.maybeAddAsyncListeners(listenableFuture, asyncListeners);
        return asyncUtils.buildInterruptible(listenableFuture);
    }

    protected Function<Row, T> rowToEntity() {
        return new Function<Row, T>() {
            @Override
            public T apply(Row row) {
                T entity = null;
                if (row != null) {
                    entity = mapper.mapRowToEntityWithPrimaryKey(meta, row, propertiesMap, entityState);
                }
                return entity;
            }
        };
    }

    protected Function<T, T> applyTriggersToEntity() {
        return new Function<T, T>() {
            @Override
            public T apply(T entity) {
                if (entity != null) {
                    meta.forInterception().intercept(entity, Event.POST_LOAD);
                }
                return entity;
            }
        };
    }

    protected Function<T, T> proxifyEntity() {
        return new Function<T, T>() {
            @Override
            public T apply(T entity) {
                T newEntity = entity;
                if (entity != null && entityState.isManaged()) {
                    newEntity = buildProxy(entity);
                }
                return newEntity;
            }
        };
    }


    protected Function<RowsWithExecutionInfo, EntitiesWithExecutionInfo<T>> rowsToEntities() {
        return new Function<RowsWithExecutionInfo, EntitiesWithExecutionInfo<T>>() {
            @Override
            public EntitiesWithExecutionInfo<T> apply(RowsWithExecutionInfo rows) {
                List<T> entities = new ArrayList<>();
                for (Row row : rows.getRows()) {
                    entities.add(rowToEntity().apply(row));
                }
                return new EntitiesWithExecutionInfo<>(from(entities).filter(notNull()).toList(), rows.getExecutionInfo());
            }
        };
    }

    protected Function<EntitiesWithExecutionInfo<T>, EntitiesWithExecutionInfo<T>> applyTriggersToEntities() {
        return new Function<EntitiesWithExecutionInfo<T>, EntitiesWithExecutionInfo<T>>() {
            @Override
            public EntitiesWithExecutionInfo<T> apply(EntitiesWithExecutionInfo<T> entities) {
                for (T entity : entities.getEntities()) {
                    applyTriggersToEntity().apply(entity);
                }
                return entities;
            }
        };
    }

    protected Function<EntitiesWithExecutionInfo<T>, EntitiesWithExecutionInfo<T>> proxifyEntities() {
        return new Function<EntitiesWithExecutionInfo<T>, EntitiesWithExecutionInfo<T>>() {
            @Override
            public EntitiesWithExecutionInfo<T> apply(EntitiesWithExecutionInfo<T> entities) {
                List<T> proxies = new ArrayList<>();
                for (T entity : entities.getEntities()) {
                    proxies.add(proxifyEntity().apply(entity));
                }
                return new EntitiesWithExecutionInfo<>(from(proxies).filter(notNull()).toList(),entities.getExecutionInfo());
            }
        };
    }

    private Map<String, PropertyMeta> transformPropertiesMap(EntityMeta meta) {
        Map<String, PropertyMeta> propertiesMap = new HashMap<>();
        for (PropertyMeta pm : meta.getPropertyMetas().values()) {
            if (!pm.type().isCompoundPK()) {
                String cqlColumnName = pm.getCQLColumnName().replaceAll("\"", "");
                propertiesMap.put(cqlColumnName, pm);
            }
        }
        return propertiesMap;
    }
    private T buildProxy(T entity) {
        PersistenceContext context = contextFactory.newContext(entity);
        entity = proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context.getEntityFacade());
        return entity;
    }
}

