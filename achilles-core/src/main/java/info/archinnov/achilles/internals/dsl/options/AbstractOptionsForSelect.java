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

package info.archinnov.achilles.internals.dsl.options;

import static java.util.Arrays.asList;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.policies.RetryPolicy;

import info.archinnov.achilles.internals.options.CassandraOptions;

public abstract class AbstractOptionsForSelect<T extends AbstractOptionsForSelect<T>> {

    protected abstract T getThis();

    protected abstract CassandraOptions getOptions();

    /**
     * Set the given consistency level on the generated statement
     * @throws NullPointerException if consistencyLevel is null
     */
    public T withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        getOptions().setCl(Optional.of(consistencyLevel));
        return getThis();
    }

    /**
     * Set the given consistency level on the generated statement IF NOT NULL
     */
    public T withOptionalConsistencyLevel(Optional<ConsistencyLevel> consistencyLevel) {
        getOptions().setCl(consistencyLevel);
        return getThis();
    }

    /**
     * Set the given serial consistency level on the generated statement
     * @throws NullPointerException if serialConsistencyLevel is null
     */
    public T withSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
        getOptions().setSerialCL(Optional.of(serialConsistencyLevel));
        return getThis();
    }

    /**
     * Set the given serial consistency level on the generated statement IF NOT NULL
     */
    public T withOptionalSerialConsistencyLevel(Optional<ConsistencyLevel> serialConsistencyLevel) {
        getOptions().setSerialCL(serialConsistencyLevel);
        return getThis();
    }

    /**
     * Set the given fetch size on the generated statement
     */
    public T withFetchSize(int fetchSize) {
        getOptions().setFetchSize(Optional.of(fetchSize));
        return getThis();
    }

    /**
     * Hint the current statement as idempotent. Useful for retry strategy
     */
    public T isIdempotent() {
        getOptions().setIdempotent(Optional.of(true));
        return getThis();
    }

    /**
     * Give a hint whether the current statement is idempotent. Useful for retry strategy
     */
    public T isIdempotent(boolean idempotent) {
        getOptions().setIdempotent(Optional.of(idempotent));
        return getThis();
    }

    /**
     * Set the given outgoing payload map on the generated statement
     * @throws NullPointerException if outgoingPayload is null
     */
    public T withOutgoingPayload(Map<String, ByteBuffer> outgoingPayload) {
        getOptions().setOutgoingPayLoad(Optional.of(outgoingPayload));
        return getThis();
    }

    /**
     * Set the given outgoing payload map on the generated statement IF NOT NULL
     */
    public T withOptionalOutgoingPayload(Optional<Map<String, ByteBuffer>> outgoingPayload) {
        getOptions().setOutgoingPayLoad(outgoingPayload);
        return getThis();
    }

    /**
     * Set the given paging state on the generated statement
     * @throws NullPointerException if pagingState is null
     */
    public T withPagingState(PagingState pagingState) {
        getOptions().setPagingState(Optional.of(pagingState));
        return getThis();
    }


    /**
     * Set the given paging state string on the generated statement
     * @throws NullPointerException if paging state string is null
     */
    public T withPagingState(String pagingState) {
        getOptions().setPagingState(Optional.of(PagingState.fromString(pagingState)));
        return getThis();
    }


    /**
     * Set the given paging state on the generated statement IF NOT NULL
     */
    public T withOptionalPagingState(Optional<PagingState> pagingState) {
        getOptions().setPagingState(pagingState);
        return getThis();
    }

    /**
     * Set the given paging state string on the generated statement IF NOT NULL
     */
    public T withOptionalPagingStateString(Optional<String> pagingStateString) {
        pagingStateString.ifPresent(cl -> getOptions().setPagingState(Optional.of(PagingState.fromString(pagingStateString.get()))));
        return getThis();
    }

    /**
     * Set the given retry policy
     * @throws NullPointerException if value is null
     */
    public T withRetryPolicy(RetryPolicy retryPolicy) {
        getOptions().setRetryPolicy(Optional.of(retryPolicy));
        return getThis();
    }

    /**
     * Set the given retry policy
     */
    public T withOptionalRetryPolicy(Optional<RetryPolicy> retryPolicy) {
        getOptions().setRetryPolicy(retryPolicy);
        return getThis();
    }

    /**
     * Add the given list of async listeners on the {@link com.datastax.driver.core.ResultSet} object.
     * Example of usage:
     * <pre class="code"><code class="java">

     * .withResultSetAsyncListeners(Arrays.asList(resultSet -> {
     * //Do something with the resultSet object here
     * }))

     * </code></pre>

     * Remark: <strong>it is not allowed to consume the ResultSet values. It is strongly advised to read only meta data</strong>
     */
    public T withResultSetAsyncListeners(List<Function<ResultSet, ResultSet>> resultSetAsyncListeners) {
        getOptions().setResultSetAsyncListeners(Optional.of(resultSetAsyncListeners));
        return getThis();
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
    public T withResultSetAsyncListener(Function<ResultSet, ResultSet> resultSetAsyncListener) {
        getOptions().setResultSetAsyncListeners(Optional.of(asList(resultSetAsyncListener)));
        return getThis();
    }

    /**
     * Add the given list of async listeners on the {@link com.datastax.driver.core.Row} object.
     * Example of usage:
     * <pre class="code"><code class="java">

     * .withRowAsyncListeners(Arrays.asList(row -> {
     * //Do something with the row object here
     * }))

     * </code></pre>

     * Remark: <strong>You can inspect and read values from the row object</strong>
     */
    public T withRowAsyncListeners(List<Function<Row, Row>> rowAsyncListeners) {
        getOptions().setRowAsyncListeners(Optional.of(rowAsyncListeners));
        return getThis();
    }

    /**
     * Add the given async listener on the {@link com.datastax.driver.core.Row} object.
     * Example of usage:
     * <pre class="code"><code class="java">

     * .withRowAsyncListener(row -> {
     * //Do something with the row object here
     * })

     * </code></pre>

     * Remark: <strong>You can inspect and read values from the row object</strong>
     */
    public T withRowAsyncListener(Function<Row, Row> rowAsyncListener) {
        getOptions().setRowAsyncListeners(Optional.of(asList(rowAsyncListener)));
        return getThis();
    }

    /**
     * Enable query tracing.
     * Please configure the logger <strong>ACHILLES_DML_STATEMENT</strong> at <strong>TRACE</strong>
     * level to see tracing results. Alternatively you can configure a single entity logger to restrict
     * tracing display only to this entity
     */
    public T withTracing(boolean tracing) {
        getOptions().setTracing(Optional.of(tracing));
        return getThis();
    }

    /**
     * Enable query tracing.
     * Please configure the logger <strong>ACHILLES_DML_STATEMENT</strong> at <strong>TRACE</strong>
     * level to see tracing results. Alternatively you can configure a single entity logger to restrict
     * tracing display only to this entity
     */
    public T withTracing() {
        getOptions().setTracing(Optional.of(true));
        return getThis();
    }

    /**
     * Set read timeout in millisecs.
     * <br/>
     * This paramater is useful when using user-defined aggregates. You may want to increase the read timeout if the aggregates is expected to take longer than the defaut timeout
     * @param readTimeoutInMillis read timeout in millis
     */
    public T withReadTimeoutInMillis(Integer readTimeoutInMillis) {
        getOptions().setReadTimeout(readTimeoutInMillis);
        return getThis();
    }

    /**
     * When DEBUG log is enabled, restrict the Results Display to maximum <strong>DMLResultsDisplaySize</strong> rows.
     * <br/>
     * <br/>
     * <strong>WARNING: there is a hard-limit of maximum 100 rows. If you provide a value greater than 100 the number of displayed returned rows will still be capped to 100.
     * If you provide a negative number, it will default to 0
     * </strong>
     * @param DMLResultsDisplaySize the maximum number of returned rows to be displayed
     */
    public T withDMLResultsDisplaySize(int DMLResultsDisplaySize) {
        getOptions().setDMLResultsDisplaySize(Optional.of(Integer.max(0,Integer.min(DMLResultsDisplaySize, CassandraOptions.MAX_RESULTS_DISPLAY_SIZE))));
        return getThis();
    }
}
