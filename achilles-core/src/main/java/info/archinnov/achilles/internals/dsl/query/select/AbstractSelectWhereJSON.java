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

package info.archinnov.achilles.internals.dsl.query.select;

import static java.lang.String.format;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Select;

import info.archinnov.achilles.internals.dsl.StatementProvider;
import info.archinnov.achilles.internals.dsl.action.SelectJSONAction;
import info.archinnov.achilles.internals.dsl.options.AbstractOptionsForSelect;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.BoundStatementWrapper;
import info.archinnov.achilles.internals.statements.OperationType;
import info.archinnov.achilles.internals.statements.StatementWrapper;
import info.archinnov.achilles.internals.types.JSONIteratorWrapper;
import info.archinnov.achilles.type.tuples.Tuple2;

public abstract class AbstractSelectWhereJSON<T extends AbstractSelectWhereJSON<T, ENTITY>, ENTITY>
        extends AbstractOptionsForSelect<T>
        implements SelectJSONAction, StatementProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSelectWhereJSON.class);

    protected final Select.Where where;
    protected final CassandraOptions cassandraOptions;

    protected AbstractSelectWhereJSON(Select.Where where, CassandraOptions cassandraOptions) {
        this.where = where;
        this.cassandraOptions = cassandraOptions;
    }

    protected abstract List<Object> getBoundValuesInternal();

    protected abstract List<Object> getEncodedValuesInternal();

    protected abstract AbstractEntityProperty<ENTITY> getMetaInternal();

    protected abstract Class<ENTITY> getEntityClass();

    protected abstract RuntimeEngine getRte();



    /***************************************************************************************
     * JSON API                                                                         *
     ***************************************************************************************/
    @Override
    public CompletableFuture<Tuple2<List<String>, ExecutionInfo>> getListJSONAsyncWithStats() {
        final RuntimeEngine rte = getRte();
        final CassandraOptions options = getOptions();

        final StatementWrapper statementWrapper = getInternalBoundStatementWrapper();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Select async with execution info : %s",
                    statementWrapper.getBoundStatement().preparedStatement().getQueryString()));
        }
        CompletableFuture<ResultSet> futureRS = rte.execute(statementWrapper);

        return futureRS
                .thenApply(options::resultSetAsyncListener)
                .thenApply(x -> statementWrapper.logReturnResults(x, options.computeMaxDisplayedResults(rte.configContext)))
                .thenApply(statementWrapper::logTrace)
                .thenApply(resultSet -> Tuple2.of(IntStream
                        .range(0, resultSet.getAvailableWithoutFetching())
                        .mapToObj(index -> resultSet.one().getString("[json]"))
                        .collect(Collectors.toList()), resultSet.getExecutionInfo()));
    }

    @Override
    public Iterator<String> iterator() {
        final RuntimeEngine rte = getRte();
        final CassandraOptions cassandraOptions = getOptions();
        final StatementWrapper statementWrapper = getInternalBoundStatementWrapper();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Execute native query async with execution info : %s",
                    statementWrapper.getBoundStatement().preparedStatement().getQueryString()));
        }

        CompletableFuture<ResultSet> futureRS = rte.execute(statementWrapper);

        return new JSONIteratorWrapper(futureRS, statementWrapper, cassandraOptions);
    }

    @Override
    public Tuple2<Iterator<String>, ExecutionInfo> iteratorWithExecutionInfo() {
        final JSONIteratorWrapper iterator = (JSONIteratorWrapper)this.iterator();
        return Tuple2.of(iterator, iterator.getExecutionInfo());
    }

    /***************************************************************************************
     * Utility API                                                                         *
     ***************************************************************************************/
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

    protected StatementWrapper getInternalBoundStatementWrapper() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Get bound statement wrapper"));
        }

        final RuntimeEngine rte = getRte();
        final AbstractEntityProperty<ENTITY> meta = getMetaInternal();
        final CassandraOptions cassandraOptions = getOptions();

        final PreparedStatement ps = rte.prepareDynamicQuery(where);

        final StatementWrapper statementWrapper = new BoundStatementWrapper(OperationType.SELECT,
                meta, ps,
                getBoundValuesInternal().toArray(),
                getEncodedValuesInternal().toArray());

        statementWrapper.applyOptions(cassandraOptions);
        return statementWrapper;
    }
}
