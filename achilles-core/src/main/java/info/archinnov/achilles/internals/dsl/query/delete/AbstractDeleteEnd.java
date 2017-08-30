/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

package info.archinnov.achilles.internals.dsl.query.delete;


import static info.archinnov.achilles.internals.dsl.LWTHelper.triggerLWTListeners;
import static java.lang.String.format;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Delete;

import info.archinnov.achilles.internals.dsl.StatementProvider;
import info.archinnov.achilles.internals.dsl.action.MutationAction;
import info.archinnov.achilles.internals.dsl.options.AbstractOptionsForUpdateOrDelete;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.BoundStatementWrapper;
import info.archinnov.achilles.internals.statements.OperationType;
import info.archinnov.achilles.internals.statements.StatementWrapper;

public abstract class AbstractDeleteEnd<T extends AbstractDeleteEnd<T, ENTITY>, ENTITY>
        extends AbstractOptionsForUpdateOrDelete<T> implements MutationAction, StatementProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDeleteEnd.class);

    protected final Delete.Where where;
    protected final CassandraOptions cassandraOptions;

    protected AbstractDeleteEnd(Delete.Where where, CassandraOptions cassandraOptions) {
        this.where = where;
        this.cassandraOptions = cassandraOptions;
    }

    protected abstract List<Object> getBoundValuesInternal();

    protected abstract List<Object> getEncodedValuesInternal();

    protected abstract AbstractEntityProperty<ENTITY> getMetaInternal();

    protected abstract Class<ENTITY> getEntityClass();

    protected abstract RuntimeEngine getRte();

    public T ifExists(boolean ifExists) {
        if (ifExists) {
            where.ifExists();
        }
        return getThis();
    }

    public T ifExists() {
        where.ifExists();
        return getThis();
    }


    public CompletableFuture<ExecutionInfo> executeAsyncWithStats() {

        final RuntimeEngine rte = getRte();
        final CassandraOptions options = getOptions();

        final StatementWrapper statementWrapper = getInternalBoundStatementWrapper();
        final String queryString = statementWrapper.getBoundStatement().preparedStatement().getQueryString();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Execute delete async with execution info : %s", queryString));
        }

        CompletableFuture<ResultSet> futureRS = rte.execute(statementWrapper);

        return futureRS
                .thenApply(options::resultSetAsyncListener)
                .thenApply(statementWrapper::logTrace)
                .thenApply(x -> triggerLWTListeners(lwtResultListeners, x, queryString))
                .thenApply(x -> x.getExecutionInfo());
    }

    @Override
    public BoundStatement generateAndGetBoundStatement() {
        return getInternalBoundStatementWrapper().getBoundStatement();
    }

    @Override
    public String getStatementAsString() {
        return where.getQueryString();
    }


    @Override
    public List<Object> getBoundValues() {
        return getBoundValuesInternal();
    }

    @Override
    public List<Object> getEncodedBoundValues() {
        return getEncodedValuesInternal();
    }

    private StatementWrapper getInternalBoundStatementWrapper() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Get bound statement wrapper"));
        }

        final RuntimeEngine rte = getRte();
        final AbstractEntityProperty<ENTITY> meta = getMetaInternal();
        final CassandraOptions cassandraOptions = getOptions();

        PreparedStatement ps = rte.prepareDynamicQuery(where);

        StatementWrapper statementWrapper = new BoundStatementWrapper(OperationType.DELETE,
                meta, ps,
                getBoundValuesInternal().toArray(),
                getEncodedValuesInternal().toArray());

        statementWrapper.applyOptions(cassandraOptions);
        return statementWrapper;
    }
}
