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
import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.google.common.base.Predicates.isNull;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.FluentIterable.from;
import static java.util.Arrays.asList;
import java.util.List;

import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.type.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

public class ImmediateFlushContext extends AbstractFlushContext {
    private static final Logger log = LoggerFactory.getLogger(ImmediateFlushContext.class);

    public ImmediateFlushContext(DaoContext daoContext, ConsistencyLevel consistencyLevel, Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevel) {
        super(daoContext, consistencyLevel, serialConsistencyLevel);
    }

    private ImmediateFlushContext(DaoContext daoContext, List<AbstractStatementWrapper> statementWrappers,
            ConsistencyLevel consistencyLevel, Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevel) {
        super(daoContext, statementWrappers, consistencyLevel, serialConsistencyLevel);
    }

    @Override
    public void startBatch() {
        throw new UnsupportedOperationException("Cannot start a batch with a normal PersistenceManager. Please create a Batch instead");
    }

    @Override
    public ListenableFuture<Empty> flushBatch() {
        throw new UnsupportedOperationException("Cannot end a batch with a normal PersistenceManager. Please create a Batch instead");
    }

    @Override
    public ListenableFuture<List<ResultSet>> flush() {
        log.debug("Flush immediately all pending statements");

        final ListenableFuture<ResultSet> resultSetFutureFields = executeBatch(UNLOGGED, statementWrappers);
        final ListenableFuture<ResultSet> resultSetFutureCounters = executeBatch(COUNTER, counterStatementWrappers);
        final List<ListenableFuture<ResultSet>> resultSetFutures = from(asList(resultSetFutureFields, resultSetFutureCounters)).filter(not(isNull())).toList();

        return asyncUtils.mergeResultSetFutures(resultSetFutures);
    }

    @Override
    public FlushType type() {
        return FlushType.IMMEDIATE;
    }

    @Override
    public ImmediateFlushContext duplicate() {
        log.trace("Duplicate immediate flushing context");
        return new ImmediateFlushContext(daoContext, statementWrappers, consistencyLevel, serialConsistencyLevel);
    }

    @Override
    public void triggerInterceptor(EntityMeta meta, Object entity, Event event) {
        meta.forInterception().intercept(entity, event);
    }
}
