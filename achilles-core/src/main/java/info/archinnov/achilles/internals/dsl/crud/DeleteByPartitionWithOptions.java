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

import static info.archinnov.achilles.internals.cache.CacheKey.Operation.DELETE_BY_PARTITION;
import static info.archinnov.achilles.internals.dsl.LWTHelper.triggerLWTListeners;
import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import info.archinnov.achilles.internals.cache.CacheKey;
import info.archinnov.achilles.internals.dsl.StatementProvider;
import info.archinnov.achilles.internals.dsl.action.MutationAction;
import info.archinnov.achilles.internals.dsl.options.AbstractOptionsForUpdateOrDelete;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.BoundStatementWrapper;
import info.archinnov.achilles.internals.statements.OperationType;
import info.archinnov.achilles.internals.statements.StatementWrapper;

public class DeleteByPartitionWithOptions<ENTITY> extends AbstractOptionsForUpdateOrDelete<DeleteByPartitionWithOptions<ENTITY>>
        implements MutationAction, StatementProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteByPartitionWithOptions.class);

    private final AbstractEntityProperty<ENTITY> meta;
    private final RuntimeEngine rte;
    private final Object[] partitionKeys;
    private final Object[] encodedPartitionKeys;
    private final CassandraOptions options;

    public DeleteByPartitionWithOptions(AbstractEntityProperty<ENTITY> meta, RuntimeEngine rte,
                                        Object[] partitionKeys, Object[] encodedPartitionKeys,
                                        Optional<CassandraOptions> cassandraOptions) {
        this.meta = meta;
        this.rte = rte;
        this.partitionKeys = partitionKeys;
        this.encodedPartitionKeys = encodedPartitionKeys;
        this.options = cassandraOptions.orElse(new CassandraOptions());
    }

    @Override
    public CompletableFuture<ExecutionInfo> executeAsyncWithStats() {

        StatementWrapper statementWrapper = getInternalBoundStatementWrapper();
        final String queryString = statementWrapper.getBoundStatement().preparedStatement().getQueryString();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Execute delete async with execution info : %s", queryString));
        }

        CompletableFuture<ResultSet> cfutureRS = rte.execute(statementWrapper);

        return cfutureRS
                .thenApply(options::resultSetAsyncListener)
                .thenApply(statementWrapper::logTrace)
                .thenApply(x -> triggerLWTListeners(lwtResultListeners, x, queryString))
                .thenApply(x -> x.getExecutionInfo());

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
        return rte.getStaticCache(new CacheKey(meta.entityClass, DELETE_BY_PARTITION)).getQueryString();
    }

    @Override
    public List<Object> getBoundValues() {
        return Arrays.asList(partitionKeys);
    }

    @Override
    public List<Object> getEncodedBoundValues() {
        return Arrays.asList(encodedPartitionKeys);
    }

    @Override
    protected DeleteByPartitionWithOptions<ENTITY> getThis() {
        return this;
    }

    private StatementWrapper getInternalBoundStatementWrapper() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Get bound statement wrapper"));
        }

        PreparedStatement ps = DELETE_BY_PARTITION.getPreparedStatement(rte, meta, options);
        StatementWrapper statementWrapper = new BoundStatementWrapper(OperationType.DELETE, meta, ps, partitionKeys, encodedPartitionKeys);
        statementWrapper.applyOptions(options);
        return statementWrapper;
    }


}
