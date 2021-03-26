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

import static info.archinnov.achilles.internals.cache.CacheKey.Operation.*;
import static info.archinnov.achilles.internals.dsl.LWTHelper.triggerLWTListeners;
import static info.archinnov.achilles.type.interceptor.Event.POST_INSERT;
import static info.archinnov.achilles.type.interceptor.Event.PRE_INSERT;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import info.archinnov.achilles.internals.dsl.StatementProvider;
import info.archinnov.achilles.internals.dsl.action.MutationAction;
import info.archinnov.achilles.internals.dsl.options.AbstractOptionsForCRUDInsert;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.BoundValuesWrapper;
import info.archinnov.achilles.internals.statements.StatementWrapper;

public class InsertWithOptions<ENTITY> extends AbstractOptionsForCRUDInsert<InsertWithOptions<ENTITY>>
        implements MutationAction, StatementProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertWithOptions.class);

    private final AbstractEntityProperty<ENTITY> meta;
    private final RuntimeEngine rte;
    private final ENTITY instance;
    private final CassandraOptions options;
    private final boolean insertStatic;

    public InsertWithOptions(AbstractEntityProperty<ENTITY> meta, RuntimeEngine rte, ENTITY instance, boolean insertStatic, Optional<CassandraOptions> cassandraOptions) {
        this.meta = meta;
        this.rte = rte;
        this.instance = instance;
        this.insertStatic = insertStatic;
        this.options = cassandraOptions.orElse(new CassandraOptions());
    }

    public CompletableFuture<ExecutionInfo> executeAsyncWithStats() {

        meta.triggerInterceptorsForEvent(PRE_INSERT, instance);


        final StatementWrapper statementWrapper = getInternalBoundStatementWrapper();
        final String queryString = statementWrapper.getBoundStatement().preparedStatement().getQueryString();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Insert async with execution info : %s", queryString));
        }

        CompletableFuture<ResultSet> cfutureRS = rte.execute(statementWrapper);

        return cfutureRS
                .thenApply(this.options::resultSetAsyncListener)
                .thenApply(statementWrapper::logTrace)
                .thenApply(x -> triggerLWTListeners(lwtResultListeners, x, queryString))
                .thenApply(x -> x.getExecutionInfo())
                .thenApply(x -> {
                    meta.triggerInterceptorsForEvent(POST_INSERT, instance);
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
        return getInternalPreparedStatement().getQueryString();
    }

    @Override
    public List<Object> getBoundValues() {
        BoundValuesWrapper wrapper = insertStatic == true
                ? meta.extractPartitionKeysAndStaticColumnsFromEntity(instance, options)
                : meta.extractAllValuesFromEntity(instance, options);
        return wrapper.boundValuesInfo.stream().map(x -> x.boundValue).collect(toList());
    }

    @Override
    public List<Object> getEncodedBoundValues() {
        BoundValuesWrapper wrapper = insertStatic == true
                ? meta.extractPartitionKeysAndStaticColumnsFromEntity(instance, options)
                : meta.extractAllValuesFromEntity(instance, options);
        return wrapper.boundValuesInfo.stream().map(x -> x.encodedValue).collect(toList());
    }

    @Override
    protected InsertWithOptions<ENTITY> getThis() {
        return this;
    }

    private StatementWrapper getInternalBoundStatementWrapper() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Get bound statement wrapper"));
        }

        final PreparedStatement ps = getInternalPreparedStatement();
        BoundValuesWrapper wrapper = insertStatic == true
                ? meta.extractPartitionKeysAndStaticColumnsFromEntity(instance, options)
                : meta.extractAllValuesFromEntity(instance, options);

        StatementWrapper statementWrapper = wrapper.bindWithInsertStrategy(ps, getOverridenStrategy(meta));
        statementWrapper.applyOptions(options);
        return statementWrapper;
    }

    private PreparedStatement getInternalPreparedStatement() {
        if (ifNotExists.isPresent() && ifNotExists.get() == true) {
            return insertStatic == true
                    ? INSERT_STATIC_IF_NOT_EXISTS.getPreparedStatement(rte, meta, options)
                    : INSERT_IF_NOT_EXISTS.getPreparedStatement(rte, meta, options);
        } else {
            return insertStatic == true
                    ? INSERT_STATIC.getPreparedStatement(rte, meta, options)
                    : INSERT.getPreparedStatement(rte, meta, options);
        }
    }


}
