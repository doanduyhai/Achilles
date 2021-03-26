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

import static info.archinnov.achilles.internals.cache.CacheKey.Operation.INSERT_IF_NOT_EXISTS_JSON;
import static info.archinnov.achilles.internals.cache.CacheKey.Operation.INSERT_JSON;
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

import info.archinnov.achilles.internals.dsl.StatementProvider;
import info.archinnov.achilles.internals.dsl.action.MutationAction;
import info.archinnov.achilles.internals.dsl.options.AbstractOptionsForCRUDInsert;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.BoundStatementWrapper;
import info.archinnov.achilles.internals.statements.OperationType;
import info.archinnov.achilles.internals.statements.StatementWrapper;

public class InsertJSONWithOptions extends AbstractOptionsForCRUDInsert<InsertJSONWithOptions>
        implements MutationAction, StatementProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertJSONWithOptions.class);

    private final AbstractEntityProperty<?> meta;
    private final RuntimeEngine rte;
    private final String json;
    private final CassandraOptions cassandraOptions;
    private final Object[] encodedBoundValues;
    private final List<Object> encodedBoundValuesAsList;

    public InsertJSONWithOptions(AbstractEntityProperty<?> meta, RuntimeEngine rte, String json, Optional<CassandraOptions> cassandraOptions) {
        this.meta = meta;
        this.rte = rte;
        this.json = json;
        this.cassandraOptions = cassandraOptions.orElse(new CassandraOptions());
        this.encodedBoundValues = new Object[]{json};
        this.encodedBoundValuesAsList = Arrays.asList(json);
    }

    public CompletableFuture<ExecutionInfo> executeAsyncWithStats() {

        final StatementWrapper statementWrapper = getInternalBoundStatementWrapper();
        final String queryString = statementWrapper.getBoundStatement().preparedStatement().getQueryString();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Insert JSON async with execution info : %s", queryString));
        }

        CompletableFuture<ResultSet> cfutureRS = rte.execute(statementWrapper);

        return cfutureRS
                .thenApply(getOptions()::resultSetAsyncListener)
                .thenApply(statementWrapper::logTrace)
                .thenApply(x -> triggerLWTListeners(lwtResultListeners, x, queryString))
                .thenApply(x -> x.getExecutionInfo());
    }

    @Override
    protected CassandraOptions getOptions() {
        return cassandraOptions;
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
        return encodedBoundValuesAsList;
    }

    @Override
    public List<Object> getEncodedBoundValues() {
        return encodedBoundValuesAsList;
    }

    @Override
    protected InsertJSONWithOptions getThis() {
        return this;
    }

    private StatementWrapper getInternalBoundStatementWrapper() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Get bound statement wrapper"));
        }

        final PreparedStatement ps = getInternalPreparedStatement();
        final BoundStatement bs = ps.bind(json);

        StatementWrapper statementWrapper = new BoundStatementWrapper(OperationType.INSERT, meta, bs, encodedBoundValues);
        statementWrapper.applyOptions(getOptions());
        return statementWrapper;
    }

    private PreparedStatement getInternalPreparedStatement() {
        if (ifNotExists.isPresent() && ifNotExists.get() == true) {
            return INSERT_IF_NOT_EXISTS_JSON.getPreparedStatement(rte, meta, getOptions());
        } else {
            return INSERT_JSON.getPreparedStatement(rte, meta, getOptions());
        }
    }


}
