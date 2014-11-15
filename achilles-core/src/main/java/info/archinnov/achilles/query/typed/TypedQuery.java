/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.query.typed;

import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROW;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityMapper;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.statement.wrapper.NativeStatementWrapper;
import info.archinnov.achilles.listener.CASResultListener;

public class TypedQuery<T> {
    private static final Logger log = LoggerFactory.getLogger(TypedQuery.class);
    private static final Optional<CASResultListener> NO_LISTENER = Optional.absent();
    private final NativeStatementWrapper nativeStatementWrapper;

    private DaoContext daoContext;
    private ExecutorService executorService;
    private Map<String, PropertyMeta> propertiesMap;
    private EntityMeta meta;
    private PersistenceContextFactory contextFactory;
    private EntityState entityState;
    private Object[] boundValues;

    private EntityMapper mapper = EntityMapper.Singleton.INSTANCE.get();
    private EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();
    private AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();

    public TypedQuery(Class<T> entityClass, DaoContext daoContext, ConfigurationContext configContext, RegularStatement regularStatement, EntityMeta meta,
            PersistenceContextFactory contextFactory, EntityState entityState, Object[] boundValues) {
        this.daoContext = daoContext;
        this.executorService = configContext.getExecutorService();
        this.boundValues = boundValues;
        this.nativeStatementWrapper = new NativeStatementWrapper(entityClass, regularStatement, this.boundValues, Optional.<CASResultListener>absent());
        this.meta = meta;
        this.contextFactory = contextFactory;
        this.entityState = entityState;
        this.propertiesMap = transformPropertiesMap(meta);
    }

    /**
     * Executes the query and returns entities
     * <p/>
     * Matching CQL rows are mapped to entities by reflection. All un-mapped
     * columns are ignored.
     * <p/>
     * The size of the list is equal or lesser than the number of matching CQL
     * row, because some null or empty rows are ignored and filtered out
     *
     * @return List<T> list of found entities or empty list
     *
     */
    public List<T> get() {
        log.debug("Get results for typed query '{}'", nativeStatementWrapper.getStatement());
        return asyncGet().getImmediately();
    }

    /**
     * Executes the query and returns entities asynchronously
     *
     * Matching CQL rows are mapped to entities by reflection. All un-mapped
     * columns are ignored.
     *
     * The size of the list is equal or lesser than the number of matching CQL
     * row, because some null or empty rows are ignored and filtered out
     *
     * @return AchillesFuture<List<T>> future of list of found entities or empty list
     *
     */
    public AchillesFuture<List<T>> asyncGet(FutureCallback<Object>... asyncListeners) {
        log.debug("Get results asynchronously for typed query '{}'", nativeStatementWrapper.getStatement());

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);
        final ListenableFuture<List<Row>> futureRows = asyncUtils.transformFutureSync(resultSetFuture, RESULTSET_TO_ROWS);

        Function<List<Row>, List<T>> rowsToEntities = rowsToEntities();
        Function<List<T>, List<T>> applyTriggers = applyTriggersToEntities();
        Function<List<T>, List<T>> maybeCreateProxy = proxifyEntities();

        final ListenableFuture<List<T>> rawEntities = asyncUtils.transformFutureSync(futureRows, rowsToEntities);
        final ListenableFuture<List<T>> entitiesWithTriggers = asyncUtils.transformFutureSync(rawEntities, applyTriggers);

        asyncUtils.maybeAddAsyncListeners(entitiesWithTriggers, asyncListeners, executorService);

        final ListenableFuture<List<T>> maybeProxyCreated = asyncUtils.transformFutureSync(entitiesWithTriggers, maybeCreateProxy);

        return asyncUtils.buildInterruptible(maybeProxyCreated);
    }


    /**
     * Executes the query and returns first entity
     *
     * Matching CQL row is mapped to entity by reflection. All un-mapped columns
     * are ignored.
     *
     * @return T first found entity or null
     *
     */
    public T getFirst() {
        log.debug("Get first result for typed query '{}'", nativeStatementWrapper.getStatement());
        return asyncGetFirst().getImmediately();
    }

    /**
     * Executes the query and returns first entity
     *
     * Matching CQL row is mapped to entity by reflection. All un-mapped columns
     * are ignored.
     *
     * @return T first found entity or null
     *
     */
    public AchillesFuture<T> asyncGetFirst(FutureCallback<Object>... asyncListeners) {
        log.debug("Get first result asynchronously for typed query '{}'", nativeStatementWrapper.getStatement());

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);
        final ListenableFuture<Row> futureRow = asyncUtils.transformFutureSync(resultSetFuture, RESULTSET_TO_ROW);

        Function<Row, T> rowToEntity = rowToEntity();
        Function<T, T> applyTriggers = applyTriggersToEntity();
        Function<T, T> maybeCreateProxy = proxifyEntity();

        final ListenableFuture<T> rawEntity = asyncUtils.transformFutureSync(futureRow, rowToEntity);
        final ListenableFuture<T> entityWithTriggers = asyncUtils.transformFutureSync(rawEntity, applyTriggers);

        asyncUtils.maybeAddAsyncListeners(entityWithTriggers, asyncListeners, executorService);

        final ListenableFuture<T> maybeProxyCreated = asyncUtils.transformFutureSync(entityWithTriggers, maybeCreateProxy);

        return asyncUtils.buildInterruptible(maybeProxyCreated);
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


    protected Function<List<Row>, List<T>> rowsToEntities() {
        return new Function<List<Row>, List<T>>() {
            @Override
            public List<T> apply(List<Row> rows) {
                List<T> entities = new ArrayList<>();
                for (Row row : rows) {
                    entities.add(rowToEntity().apply(row));
                }
                return from(entities).filter(notNull()).toList();
            }
        };
    }

    protected Function<List<T>, List<T>> applyTriggersToEntities() {
        return new Function<List<T>, List<T>>() {
            @Override
            public List<T> apply(List<T> entities) {
                for (T entity : entities) {
                    applyTriggersToEntity().apply(entity);
                }
                return entities;
            }
        };
    }

    protected Function<List<T>, List<T>> proxifyEntities() {
        return new Function<List<T>, List<T>>() {
            @Override
            public List<T> apply(List<T> entities) {
                List<T> proxies = new ArrayList<>();
                for (T entity : entities) {
                    proxies.add(proxifyEntity().apply(entity));
                }
                return from(proxies).filter(notNull()).toList();
            }
        };
    }

    private Map<String, PropertyMeta> transformPropertiesMap(EntityMeta meta) {
        Map<String, PropertyMeta> propertiesMap = new HashMap<>();
        for (PropertyMeta pm : meta.getPropertyMetas().values()) {
            String cql3ColumnName = pm.getCQL3ColumnName();
            propertiesMap.put(cql3ColumnName, pm);
        }
        return propertiesMap;
    }
    private T buildProxy(T entity) {
        PersistenceContext context = contextFactory.newContext(entity);
        entity = proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context.getEntityFacade());
        return entity;
    }
}
