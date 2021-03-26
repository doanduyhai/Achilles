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

import static info.archinnov.achilles.internals.cache.CacheKey.Operation.DELETE;
import static info.archinnov.achilles.internals.cache.CacheKey.Operation.DELETE_IF_EXISTS;
import static info.archinnov.achilles.internals.dsl.LWTHelper.triggerLWTListeners;
import static info.archinnov.achilles.type.interceptor.Event.POST_DELETE;
import static info.archinnov.achilles.type.interceptor.Event.PRE_DELETE;
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

public class DeleteWithOptions<ENTITY> extends AbstractOptionsForUpdateOrDelete<DeleteWithOptions<ENTITY>>
        implements MutationAction, StatementProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteWithOptions.class);

    private final Class<ENTITY> entityClass;
    private final AbstractEntityProperty<ENTITY> meta;
    private final RuntimeEngine rte;
    private Object[] primaryKeyValues;
    private Object[] encodedPrimaryKeyValues;
    private final Optional<ENTITY> instance;
    private final CassandraOptions options;
    private Optional<Boolean> ifExists = Optional.empty();

    public DeleteWithOptions(Class<ENTITY> entityClass,
                             AbstractEntityProperty<ENTITY> meta,
                             RuntimeEngine rte,
                             Object[] primaryKeyValues,
                             Object[] encodedPrimaryKeyValues,
                             Optional<ENTITY> instance,
                             Optional<CassandraOptions> cassandraOptions) {
        this.entityClass = entityClass;
        this.meta = meta;
        this.rte = rte;
        this.primaryKeyValues = primaryKeyValues;
        this.encodedPrimaryKeyValues = encodedPrimaryKeyValues;
        this.instance = instance;
        this.options = cassandraOptions.orElse(new CassandraOptions());
    }

    public CompletableFuture<ExecutionInfo> executeAsyncWithStats() {

        if (this.instance.isPresent()) {
            final ENTITY entity = this.instance.get();
            meta.triggerInterceptorsForEvent(PRE_DELETE, entity);
        }

        final StatementWrapper statementWrapper = getInternalBoundStatementWrapper();
        final String queryString = statementWrapper.getBoundStatement().preparedStatement().getQueryString();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Execute delete async with execution info : %s", queryString));
        }

        CompletableFuture<ResultSet> cfutureRS = rte.execute(statementWrapper);

        return cfutureRS
                .thenApply(options::resultSetAsyncListener)
                .thenApply(statementWrapper::logTrace)
                .thenApply(x -> triggerLWTListeners(lwtResultListeners, x, queryString))
                .thenApply(x -> x.getExecutionInfo())
                .thenApply(x -> {
                    if (this.instance.isPresent()) meta.triggerInterceptorsForEvent(POST_DELETE, instance.get());
                    return x;
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
        return rte.getStaticCache(new CacheKey(entityClass, DELETE)).getQueryString();
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
    protected DeleteWithOptions<ENTITY> getThis() {
        return this;
    }

    private StatementWrapper getInternalBoundStatementWrapper() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Generate bound statement wrapper"));
        }

        final PreparedStatement ps = getInternalPreparedStatement();
        final BoundStatementWrapper statementWrapper = new BoundStatementWrapper(OperationType.DELETE, meta, ps, primaryKeyValues, encodedPrimaryKeyValues);
        statementWrapper.applyOptions(options);
        return statementWrapper;
    }

    private PreparedStatement getInternalPreparedStatement() {
        if (ifExists.isPresent() && ifExists.get() == true) {
            return DELETE_IF_EXISTS.getPreparedStatement(rte, meta, options);
        } else {
            return DELETE.getPreparedStatement(rte, meta, options);
        }
    }

    public DeleteWithOptions<ENTITY> ifExists(boolean ifExists) {
        this.ifExists = Optional.of(ifExists);
        return this;
    }

    public DeleteWithOptions<ENTITY> ifExists() {
        this.ifExists = Optional.of(true);
        return this;
    }
}
