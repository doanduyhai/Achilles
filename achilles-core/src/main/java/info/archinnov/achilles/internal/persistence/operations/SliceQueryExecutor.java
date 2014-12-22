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
package info.archinnov.achilles.internal.persistence.operations;

import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ITERATOR;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import info.archinnov.achilles.iterator.AchillesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.query.slice.SliceQueryProperties;
import info.archinnov.achilles.type.ConsistencyLevel;

public class SliceQueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(SliceQueryExecutor.class);

    protected EntityMapper mapper = EntityMapper.Singleton.INSTANCE.get();
    protected EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();
    protected AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();
    protected PersistenceContextFactory contextFactory;
    protected DaoContext daoContext;
    protected ExecutorService executorService;


    public SliceQueryExecutor(PersistenceContextFactory contextFactory, ConfigurationContext configContext, DaoContext daoContext) {
        this.contextFactory = contextFactory;
        this.daoContext = daoContext;
        this.executorService = configContext.getExecutorService();
    }

    public <T> List<T> get(SliceQueryProperties<T> sliceQueryProperties) {
        return asyncGet(sliceQueryProperties).getImmediately();
    }

    public <T> AchillesFuture<List<T>> asyncGet(SliceQueryProperties<T> sliceQueryProperties) {
        log.debug("Get slice query");
        final ListenableFuture<List<T>> futureEntities = coreAsyncGet(sliceQueryProperties);
        return asyncUtils.buildInterruptible(futureEntities);
    }

    public <T> AchillesFuture<T> asyncGetOne(SliceQueryProperties<T> sliceQueryProperties) {
        log.debug("Get slice query");

        Function<List<T>, T> takeFirstFunction = new Function<List<T>, T>() {
            @Override
            public T apply(List<T> result) {
                if (result.isEmpty()) {
                    return null;
                } else {
                    return result.get(0);
                }
            }
        };

        final ListenableFuture<List<T>> futureEntities = coreAsyncGet(sliceQueryProperties);
        final ListenableFuture<T> futureEntity = asyncUtils.transformFuture(futureEntities, takeFirstFunction);
        return asyncUtils.buildInterruptible(futureEntity);
    }

    protected <T> ListenableFuture<List<T>> coreAsyncGet(SliceQueryProperties<T> sliceQueryProperties) {
        final EntityMeta meta = sliceQueryProperties.getEntityMeta();

        final BoundStatementWrapper bsWrapper = daoContext.bindForSliceQuerySelect(sliceQueryProperties);

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(bsWrapper);
        final ListenableFuture<List<Row>> futureRows = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROWS);
        Function<List<Row>, List<T>> rowsToEntities = new Function<List<Row>, List<T>>() {
            @Override
            public List<T> apply(List<Row> rows) {
                List<T> clusteredEntities = new ArrayList<>();
                for (Row row : rows) {
                    T clusteredEntity = meta.forOperations().instanciate();
                    mapper.setNonCounterPropertiesToEntity(row, meta, clusteredEntity);
                    meta.forInterception().intercept(clusteredEntity, Event.POST_LOAD);
                    clusteredEntities.add(clusteredEntity);
                }
                return clusteredEntities;
            }
        };
        final ListenableFuture<List<T>> futureEntities = asyncUtils.transformFuture(futureRows, rowsToEntities);
        asyncUtils.maybeAddAsyncListeners(futureEntities, sliceQueryProperties.getAsyncListeners());

        return asyncUtils.transformFuture(futureEntities, this.<T>getProxyListTransformer());
    }

    public <T> Iterator<T> iterator(final SliceQueryProperties<T> sliceQueryProperties) {
        log.debug("Get iterator for slice query");
        return asyncIterator(sliceQueryProperties).getImmediately();
    }

    public <T> AchillesFuture<Iterator<T>> asyncIterator(final SliceQueryProperties<T> sliceQueryProperties) {
        log.debug("Get iterator for slice query asynchronously");
        final BoundStatementWrapper bsWrapper = daoContext.bindForSliceQuerySelect(sliceQueryProperties);
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(bsWrapper);
        final ListenableFuture<Iterator<Row>> futureIterator = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ITERATOR);

        Function<Iterator<Row>, Iterator<T>> rowToIterator = new Function<Iterator<Row>, Iterator<T>>() {
            @Override
            public Iterator<T> apply(Iterator<Row> rowIterator) {
                PersistenceContext context = buildContextForQuery(sliceQueryProperties);
                return new AchillesIterator<>(sliceQueryProperties.getEntityMeta(), context, rowIterator);
            }
        };
        final ListenableFuture<Iterator<T>> listenableFuture = asyncUtils.transformFuture(futureIterator, rowToIterator);
        asyncUtils.maybeAddAsyncListeners(listenableFuture, sliceQueryProperties.getAsyncListeners());
        return asyncUtils.buildInterruptible(listenableFuture);
    }

    public <T> void delete(final SliceQueryProperties<T> sliceQueryProperties) {
        asyncDelete(sliceQueryProperties).getImmediately();
    }

    public <T> AchillesFuture<Empty> asyncDelete(final SliceQueryProperties<T> sliceQueryProperties) {
        log.debug("Slice delete");
        final BoundStatementWrapper bsWrapper = daoContext.bindForSliceQueryDelete(sliceQueryProperties);
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(bsWrapper);
        final ListenableFuture<Empty> listenableFuture = asyncUtils.transformFutureToEmpty(resultSetFuture, executorService);
        asyncUtils.maybeAddAsyncListeners(listenableFuture, sliceQueryProperties.getAsyncListeners(), executorService);
        return asyncUtils.buildInterruptible(listenableFuture);
    }

    protected <T> PersistenceContext buildContextForQuery( SliceQueryProperties<T> sliceQueryProperties) {
        log.trace("Build PersistenceContext for slice query");
        ConsistencyLevel cl = sliceQueryProperties.getReadConsistencyLevel();
        return contextFactory.newContextForSliceQuery(sliceQueryProperties.getEntityClass(), sliceQueryProperties.getPartitionKeys(), cl);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    private <T> Function<List<T>, List<T>> getProxyListTransformer() {
        return new Function<List<T>, List<T>>() {
            @Override
            public List<T> apply(List<T> clusteredEntities) {
                final List<T> proxies = new ArrayList<>();
                for (T clusteredEntity : clusteredEntities) {
                    PersistenceContext context = contextFactory.newContext(clusteredEntity);
                    proxies.add(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(clusteredEntity, context.getEntityFacade()));
                }
                return proxies;
            }
        };
    }
}
