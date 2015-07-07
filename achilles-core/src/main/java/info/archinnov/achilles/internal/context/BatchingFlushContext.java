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
package info.archinnov.achilles.internal.context;

import static com.datastax.driver.core.BatchStatement.Type.COUNTER;
import static com.google.common.base.Predicates.isNull;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.FluentIterable.from;
import static java.util.Arrays.asList;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.type.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.EmptyFutureResultSets;
import info.archinnov.achilles.internal.interceptor.EventHolder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

public class BatchingFlushContext extends AbstractFlushContext {

    private static final Logger log = LoggerFactory.getLogger(BatchingFlushContext.class);
    protected List<EventHolder> eventHolders = new ArrayList<>();
    protected final BatchStatement.Type batchType;

    public BatchingFlushContext(DaoContext daoContext, ConsistencyLevel consistencyLevel,
            Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevel, BatchStatement.Type batchType) {
        super(daoContext, consistencyLevel, serialConsistencyLevel);
        this.batchType = batchType;
    }

    private BatchingFlushContext(DaoContext daoContext, List<AbstractStatementWrapper> statementWrappers,
            ConsistencyLevel consistencyLevel, Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevel, BatchStatement.Type batchType) {
        super(daoContext, statementWrappers, consistencyLevel, serialConsistencyLevel);
        this.batchType = batchType;
    }

    @Override
    public void startBatch() {
        log.debug("Starting a new batch");
    }

    @Override
    public ListenableFuture<List<ResultSet>> flush() {
        log.debug("Flush called but do nothing. Flushing is done only at the end of the batch");
        return EmptyFutureResultSets.instance();
    }

    @Override
    public AchillesFuture<Empty> flushBatch() {
        log.debug("Ending current batch");

        Function<List<ResultSet>, Empty> applyTriggers = new Function<List<ResultSet>, Empty>() {
            @Override
            public Empty apply(List<ResultSet> input) {
                for (EventHolder eventHolder : eventHolders) {
                    eventHolder.triggerInterception();
                }
                return Empty.INSTANCE;
            }
        };

        final ListenableFuture<ResultSet> resultSetFutureFields = executeBatch(batchType, statementWrappers);
        final ListenableFuture<ResultSet> resultSetFutureCounters = executeBatch(COUNTER, counterStatementWrappers);
        final List<ListenableFuture<ResultSet>> resultSetFutures = from(asList(resultSetFutureFields, resultSetFutureCounters)).filter(not(isNull())).toList();

        final ListenableFuture<List<ResultSet>> futureAsList = asyncUtils.mergeResultSetFutures(resultSetFutures);
        final ListenableFuture<Empty> triggersApplied = asyncUtils.transformFuture(futureAsList, applyTriggers);
        return asyncUtils.buildInterruptible(triggersApplied);
    }

    @Override
    public FlushType type() {
        return FlushType.BATCH;
    }

    @Override
    public BatchingFlushContext duplicate() {
        return new BatchingFlushContext(daoContext, statementWrappers, consistencyLevel, serialConsistencyLevel, batchType);
    }

    @Override
    public void triggerInterceptor(EntityMeta meta, Object entity, Event event) {
        if (event == Event.POST_LOAD) {
            meta.forInterception().intercept(entity, Event.POST_LOAD);
        } else {
            this.eventHolders.add(new EventHolder(meta, entity, event));
        }
    }

    public BatchingFlushContext duplicateWithNoData(ConsistencyLevel defaultConsistencyLevel) {
        return new BatchingFlushContext(daoContext, new ArrayList<AbstractStatementWrapper>(), defaultConsistencyLevel, serialConsistencyLevel, batchType);
    }

    public BatchingFlushContext duplicateWithNoData(ConsistencyLevel defaultConsistencyLevel,
            Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevel ) {
        return new BatchingFlushContext(daoContext, new ArrayList<AbstractStatementWrapper>(), defaultConsistencyLevel, serialConsistencyLevel, batchType);
    }
}
