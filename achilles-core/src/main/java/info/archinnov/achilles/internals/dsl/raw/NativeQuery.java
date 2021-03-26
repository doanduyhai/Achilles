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

package info.archinnov.achilles.internals.dsl.raw;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;

import info.archinnov.achilles.internals.dsl.LWTHelper;
import info.archinnov.achilles.internals.dsl.RawAndTypeMapDefaultImpl;
import info.archinnov.achilles.internals.dsl.action.MutationAction;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.NativeStatementWrapper;
import info.archinnov.achilles.internals.statements.StatementWrapper;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener;

/**
 * Native query
 */
public class NativeQuery implements MutationAction, RawAndTypeMapDefaultImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeQuery.class);

    private final AbstractEntityProperty<?> meta;
    private final RuntimeEngine rte;
    private final BoundStatement boundStatement;
    private final Object[] encodedBoundValues;
    private final CassandraOptions options = new CassandraOptions();
    private Optional<List<LWTResultListener>> lwtResultListeners = Optional.empty();

    public NativeQuery(AbstractEntityProperty<?> meta, RuntimeEngine rte, BoundStatement boundStatement, Object[] encodedBoundValues) {
        this.meta = meta;
        this.rte = rte;
        this.boundStatement = boundStatement;
        this.encodedBoundValues = encodedBoundValues;
    }

    /**
     * Add a list of LWT result listeners. Example of usage:
     * <pre class="code"><code class="java">
     * LWTResultListener lwtListener = new LWTResultListener() {
     *
     *  public void onError(LWTResult lwtResult) {
     *
     *      //Get type of LWT operation that fails
     *      LWTResult.Operation operation = lwtResult.operation();
     *
     *      // Print out current values
     *      TypedMap currentValues = lwtResult.currentValues();
     *      currentValues
     *          .entrySet()
     *          .forEach(entry -> System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue())));
     *  }
     * };
     * </code></pre>
     */
    public NativeQuery withLwtResultListeners(List<LWTResultListener> lwtResultListeners) {
        this.lwtResultListeners = Optional.of(lwtResultListeners);
        return this;
    }

    /**
     * Add a single LWT result listeners. Example of usage:
     * <pre class="code"><code class="java">
     * LWTResultListener lwtListener = new LWTResultListener() {
     *
     *  public void onError(LWTResult lwtResult) {
     *
     *      //Get type of LWT operation that fails
     *      LWTResult.Operation operation = lwtResult.operation();
     *
     *      // Print out current values
     *      TypedMap currentValues = lwtResult.currentValues();
     *      currentValues
     *          .entrySet()
     *          .forEach(entry -> System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue())));
     *  }
     * };
     * </code></pre>
     */
    public NativeQuery withLwtResultListener(LWTResultListener lwtResultListener) {
        this.lwtResultListeners = Optional.of(asList(lwtResultListener));
        return this;
    }

    /**
     * Add the given list of async listeners on the {@link com.datastax.driver.core.ResultSet} object.
     * Example of usage:
     * <pre class="code"><code class="java">
     *
     * .withResultSetAsyncListeners(Arrays.asList(resultSet -> {
     *      //Do something with the resultSet object here
     * }))
     *
     * </code></pre>
     *
     * Remark: <strong>it is not allowed to consume the ResultSet values. It is strongly advised to read only meta data</strong>
     */
    public NativeQuery withResultSetAsyncListeners(List<Function<ResultSet, ResultSet>> resultSetAsyncListeners) {
        this.options.setResultSetAsyncListeners(Optional.of(resultSetAsyncListeners));
        return this;
    }

    /**
     * Add the given async listener on the {@link com.datastax.driver.core.ResultSet} object.
     * Example of usage:
     * <pre class="code"><code class="java">

     * .withResultSetAsyncListener(resultSet -> {
     * //Do something with the resultSet object here
     * })

     * </code></pre>

     * Remark: <strong>it is not allowed to consume the ResultSet values. It is strongly advised to read only meta data</strong>
     */
    public NativeQuery withResultSetAsyncListener(Function<ResultSet, ResultSet> resultSetAsyncListener) {
        this.options.setResultSetAsyncListeners(Optional.of(asList(resultSetAsyncListener)));
        return this;
    }

    /**
     * When DEBUG log is enabled, restrict the Results Display to maximum <strong>DMLResultsDisplaySize</strong> rows. This only applies to SELECT statements
     * <br/>
     * <br/>
     * <strong>WARNING: there is a hard-limit of maximum 100 rows. If you provide a value greater than 100 the number of displayed returned rows will still be capped to 100.
     * If you provide a negative number, it will default to 0
     * </strong>
     * @param DMLResultsDisplaySize the maximum number of returned rows to be displayed
     */
    public NativeQuery withDMLResultsDisplaySize(int DMLResultsDisplaySize) {
        if (!getOperationType(boundStatement).isUpsert) {
            options.setDMLResultsDisplaySize(Optional.of(Integer.max(0,Integer.min(DMLResultsDisplaySize, CassandraOptions.MAX_RESULTS_DISPLAY_SIZE))));
        }
        return this;
    }

    /**
     * Execute the native query asynchronously and return the execution info
     *
     * @return CompletableFuture&lt;ExecutionInfo&gt;
     */
    @Override
    public CompletableFuture<ExecutionInfo> executeAsyncWithStats() {

        final StatementWrapper statementWrapper = new NativeStatementWrapper(getOperationType(boundStatement), meta, boundStatement, encodedBoundValues);
        final String queryString = statementWrapper.getBoundStatement().preparedStatement().getQueryString();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Execute native query async with execution info : %s", queryString));
        }

        CompletableFuture<ResultSet> cfutureRS = rte.execute(statementWrapper);

        return cfutureRS
                .thenApply(options::resultSetAsyncListener)
                .thenApply(x -> statementWrapper.logReturnResults(x, options.computeMaxDisplayedResults(rte.configContext)))
                .thenApply(statementWrapper::logTrace)
                .thenApply(x -> LWTHelper.triggerLWTListeners(lwtResultListeners, x, queryString))
                .thenApply(x -> x.getExecutionInfo());
    }


    @Override
    public RuntimeEngine runtimeEngine() {
        return rte;
    }

    @Override
    public AbstractEntityProperty<?> meta() {
        return meta;
    }

    @Override
    public BoundStatement boundStatement() {
        return boundStatement;
    }

    @Override
    public Object[] encodedBoundValues() {
        return encodedBoundValues;
    }

    @Override
    public CassandraOptions options() {
        return options;
    }
}
