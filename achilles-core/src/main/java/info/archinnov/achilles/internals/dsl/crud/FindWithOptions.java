/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.internals.dsl.crud;

import static info.archinnov.achilles.internals.cache.CacheKey.Operation.FIND;
import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.Uninterruptibles;

import info.archinnov.achilles.internals.cache.CacheKey;
import info.archinnov.achilles.internals.dsl.AsyncAware;
import info.archinnov.achilles.internals.dsl.StatementProvider;
import info.archinnov.achilles.internals.dsl.options.AbstractOptionsForSelect;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.BoundStatementWrapper;
import info.archinnov.achilles.internals.statements.OperationType;
import info.archinnov.achilles.internals.statements.StatementWrapper;
import info.archinnov.achilles.type.interceptor.Event;
import info.archinnov.achilles.type.tuples.Tuple2;

public class FindWithOptions<ENTITY> extends AbstractOptionsForSelect<FindWithOptions<ENTITY>>
        implements StatementProvider, AsyncAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(FindWithOptions.class);

    private final Class<ENTITY> entityClass;
    private final AbstractEntityProperty<ENTITY> meta;
    private final RuntimeEngine rte;
    private final Object[] primaryKeyValues;
    private final Object[] encodedPrimaryKeyValues;
    private final CassandraOptions options;

    public FindWithOptions(Class<ENTITY> entityClass, AbstractEntityProperty<ENTITY> meta, RuntimeEngine rte,
                           Object[] primaryKeyValues, Object[] encodedPrimaryKeyValues, Optional<CassandraOptions> cassandraOptions) {
        this.entityClass = entityClass;
        this.meta = meta;
        this.rte = rte;
        this.primaryKeyValues = primaryKeyValues;
        this.encodedPrimaryKeyValues = encodedPrimaryKeyValues;
        this.options = cassandraOptions.orElse(new CassandraOptions());
    }

    public ENTITY get() {
        try {
            return Uninterruptibles.getUninterruptibly(getAsync());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    public Tuple2<ENTITY, ExecutionInfo> getWithStats() {
        try {
            return Uninterruptibles.getUninterruptibly(getAsyncWithStats());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    public CompletableFuture<ENTITY> getAsync() {
        return getAsyncWithStats().thenApply(tuple2 -> tuple2._1());
    }

    public CompletableFuture<Tuple2<ENTITY, ExecutionInfo>> getAsyncWithStats() {

        StatementWrapper statementWrapper = getInternalBoundStatementWrapper();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Find async with execution info : %s",
                    statementWrapper.getBoundStatement().preparedStatement().getQueryString()));
        }
        CompletableFuture<ResultSet> futureRS = rte.execute(statementWrapper);

        return futureRS
                .thenApply(options::resultSetAsyncListener)
                .thenApply(x -> statementWrapper.logReturnResults(x, options.computeMaxDisplayedResults(rte.configContext)))
                .thenApply(statementWrapper::logTrace)
                .thenApply(rs -> {
                    final Row row = rs.one();
                    options.rowAsyncListener(row);
                    return Tuple2.of(meta.createEntityFrom(row), rs.getExecutionInfo());
                })
                .thenApply(tuple2 -> {
                    meta.triggerInterceptorsForEvent(Event.POST_LOAD, tuple2._1());
                    return tuple2;
                });
    }

    @Override
    protected CassandraOptions getOptions() {
        return options;
    }

    @Override
    public BoundStatement generateAndGetBoundStatement() {
        return getInternalBoundStatementWrapper().getBoundStatement();
    }

    @Override
    public String getStatementAsString() {
        return rte.getStaticCache(new CacheKey(entityClass, FIND)).getQueryString();
    }


    @Override
    public List<Object> getBoundValues() {
        return Arrays.asList(primaryKeyValues);
    }

    @Override
    public List<Object> getEncodedBoundValues() {
        return Arrays.asList(encodedPrimaryKeyValues);
    }

    @Override
    protected FindWithOptions<ENTITY> getThis() {
        return this;
    }

    private StatementWrapper getInternalBoundStatementWrapper() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Get bound statement wrapper"));
        }

        final PreparedStatement ps = FIND.getPreparedStatement(rte, meta, options);
        StatementWrapper statementWrapper = new BoundStatementWrapper(OperationType.SELECT, meta, ps, primaryKeyValues, encodedPrimaryKeyValues);
        statementWrapper.applyOptions(options);
        return statementWrapper;
    }
}
