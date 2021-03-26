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

package info.archinnov.achilles.internals.options;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RetryPolicy;

import info.archinnov.achilles.internals.context.ConfigurationContext;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.statements.OperationType;
import info.archinnov.achilles.internals.types.LimitedResultSetWrapper;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.validation.Validator;

public class CassandraOptions {

    public static Integer MAX_RESULTS_DISPLAY_SIZE = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraOptions.class);

    private Optional<ConsistencyLevel> cl = Optional.empty();
    private Optional<ConsistencyLevel> serialCL = Optional.empty();
    private Optional<Long> defaultTimestamp = Optional.empty();
    private Optional<Integer> timeToLive = Optional.empty();
    private Optional<Integer> fetchSize = Optional.empty();
    private Optional<Boolean> idempotent = Optional.empty();
    private Optional<Map<String, ByteBuffer>> outgoingPayLoad = Optional.empty();
    private Optional<PagingState> pagingState = Optional.empty();
    private Optional<RetryPolicy> retryPolicy = Optional.empty();
    private Optional<List<Function<ResultSet, ResultSet>>> resultSetAsyncListeners = Optional.empty();
    private Optional<List<Function<Row, Row>>> rowAsyncListeners = Optional.empty();
    private Optional<Boolean> tracing = Optional.empty();
    private Optional<SchemaNameProvider> schemaNameProvider = Optional.empty();
    private Optional<Integer> readTimeout = Optional.empty();
    private Optional<StringJoiner> dseSearchSolrQuery = Optional.empty();
    private Optional<String> dseSearchRawSolrQuery = Optional.empty();
    private Optional<Integer> DMLResultsDisplaySize = Optional.empty();

    public CassandraOptions() {}

    public static CassandraOptions withSchemaNameProvider(SchemaNameProvider schemaNameProvider) {
        final CassandraOptions cassandraOptions = new CassandraOptions();
        Validator.validateNotNull(schemaNameProvider, "The provided schemaNameProvider should not be null");
        cassandraOptions.setSchemaNameProvider(Optional.of(schemaNameProvider));
        return cassandraOptions;
    }

    public int computeMaxDisplayedResults(ConfigurationContext configContext) {
        return DMLResultsDisplaySize.orElse(configContext.getDMLResultsDisplaySize());
    }

    public void appendToSolrQuery(String solrQuery) {
        if (!dseSearchSolrQuery.isPresent()) {
            dseSearchSolrQuery = Optional.of(new StringJoiner(" AND "));
        }
        dseSearchSolrQuery = dseSearchSolrQuery.map(content -> content.add(solrQuery));
        dseSearchRawSolrQuery = Optional.empty();
    }

    public void rawSolrQuery(String rawSolrQuery) {
        this.dseSearchRawSolrQuery = Optional.of(rawSolrQuery);
        this.dseSearchSolrQuery = Optional.empty();
    }

    public boolean hasSolrQuery() {
        return dseSearchSolrQuery.isPresent() || dseSearchRawSolrQuery.isPresent();
    }

    public boolean hasRawSolrQuery() {
        return dseSearchRawSolrQuery.isPresent();
    }

    public String generateSolrQuery() {
        return dseSearchSolrQuery.get().toString();
    }

    public String generateRawSolrQuery() {
        return dseSearchRawSolrQuery.get();
    }

    public boolean hasCl() {
        return cl.isPresent();
    }

    public Optional<ConsistencyLevel> getCl() {
        return cl;
    }

    public void setCl(Optional<ConsistencyLevel> cl) {
        this.cl = cl;
    }

    public boolean hasSerialCl() {
        return serialCL.isPresent();
    }

    public Optional<ConsistencyLevel> getSerialCL() {
        return serialCL;
    }

    public void setSerialCL(Optional<ConsistencyLevel> serialCL) {
        this.serialCL = serialCL;
    }

    public boolean hasDefaultTimestamp() {
        return defaultTimestamp.isPresent();
    }

    public Optional<Long> getDefaultTimestamp() {
        return defaultTimestamp;
    }

    public void setDefaultTimestamp(Optional<Long> defaultTimestamp) {
        this.defaultTimestamp = defaultTimestamp;
    }

    public boolean hasFetchSize() {
        return fetchSize.isPresent();
    }

    public Optional<Integer> getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Optional<Integer> fetchSize) {
        this.fetchSize = fetchSize;
    }

    public boolean hasIdempotent() {
        return idempotent.isPresent();
    }

    public Optional<Boolean> getIdempotent() {
        return idempotent;
    }

    public void setIdempotent(Optional<Boolean> idempotent) {
        this.idempotent = idempotent;
    }

    public boolean hasOutgoingPayload() {
        return outgoingPayLoad.isPresent();
    }

    public Optional<Map<String, ByteBuffer>> getOutgoingPayLoad() {
        return outgoingPayLoad;
    }

    public void setOutgoingPayLoad(Optional<Map<String, ByteBuffer>> outgoingPayLoad) {
        this.outgoingPayLoad = outgoingPayLoad;
    }

    public boolean hasPagingState() {
        return pagingState.isPresent();
    }

    public Optional<PagingState> getPagingState() {
        return pagingState;
    }

    public void setPagingState(Optional<PagingState> pagingState) {
        this.pagingState = pagingState;
    }

    public boolean hasRetryPolicy() {
        return retryPolicy.isPresent();
    }

    public Optional<RetryPolicy> getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(Optional<RetryPolicy> retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public ResultSet resultSetAsyncListener(ResultSet originalResultSet) {

        final LimitedResultSetWrapper limitedRs = new LimitedResultSetWrapper(originalResultSet);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Applying Async listeners %s to the resultset %s",
                    resultSetAsyncListeners, limitedRs));
        }
        resultSetAsyncListeners
                .ifPresent(list -> list.forEach(x -> x.apply(limitedRs)));
        return originalResultSet;
    }

    public Row rowAsyncListener(Row row) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Applying Async listeners %s to row %s",
                    rowAsyncListeners, row));
        }

        rowAsyncListeners.map(list -> list.stream().map(listener -> listener.apply(row)).count()).orElse(0L);
        return row;
    }

    public Optional<List<Function<ResultSet, ResultSet>>> getResultSetAsyncListeners() {
        return resultSetAsyncListeners;
    }

    public void setResultSetAsyncListeners(Optional<List<Function<ResultSet, ResultSet>>> resultSetAsyncListeners) {
        this.resultSetAsyncListeners = resultSetAsyncListeners;
    }

    public Optional<List<Function<Row, Row>>> getRowAsyncListeners() {
        return rowAsyncListeners;
    }

    public void setRowAsyncListeners(Optional<List<Function<Row, Row>>> rowAsyncListeners) {
        this.rowAsyncListeners = rowAsyncListeners;
    }

    public Optional<Boolean> getTracing() {
        return tracing;
    }

    public void setTracing(Optional<Boolean> tracing) {
        this.tracing = tracing;
    }

    public boolean hasTTL() {
        return timeToLive.isPresent();
    }

    public Optional<Integer> getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(Optional<Integer> timeToLive) {
        this.timeToLive = timeToLive;
    }

    public boolean hasSchemaNameProvider() {
        return schemaNameProvider.isPresent();
    }

    public Optional<SchemaNameProvider> getSchemaNameProvider() {
        return schemaNameProvider;
    }

    public void setSchemaNameProvider(Optional<SchemaNameProvider> schemaNameProvider) {
        this.schemaNameProvider = schemaNameProvider;
    }

    public void setReadTimeout(Integer readTimeout) {
        this.readTimeout = Optional.ofNullable(readTimeout);
    }

    public void setDMLResultsDisplaySize(Optional<Integer> DMLResultsDisplaySize) {
        this.DMLResultsDisplaySize = DMLResultsDisplaySize;
    }

    public Statement applyOptions(OperationType operationType, AbstractEntityProperty<?> meta, Statement statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Applying options %s to the current statement %s",
                    this.toString(), statement.toString()));
        }

        if (meta.isTable()) {
            statement.setConsistencyLevel(operationType.isUpsert ? meta.writeConsistency(cl) : meta.readConsistency(cl));
            statement.setSerialConsistencyLevel(meta.serialConsistency(serialCL));
        } else if (meta.isView()) {
            statement.setConsistencyLevel(meta.readConsistency(cl));
        }


        if (defaultTimestamp.isPresent() && operationType.isUpsert && meta.isTable())
            statement.setDefaultTimestamp(defaultTimestamp.get());
        if (fetchSize.isPresent()) statement.setFetchSize(fetchSize.get());
        if (idempotent.isPresent()) statement.setIdempotent(idempotent.get());
        if (outgoingPayLoad.isPresent()) statement.setOutgoingPayload(outgoingPayLoad.get());
        if (pagingState.isPresent()) statement.setPagingState(pagingState.get());
        if (retryPolicy.isPresent()) statement.setRetryPolicy(retryPolicy.get());
        if (tracing.isPresent() || meta.entityLogger.isTraceEnabled()) statement.enableTracing();
        if (readTimeout.isPresent()) statement.setReadTimeoutMillis(readTimeout.get());

        return statement;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Options{");
        sb.append("cl=").append(cl);
        sb.append(", serialCL=").append(serialCL);
        sb.append(", defaultTimestamp=").append(defaultTimestamp);
        sb.append(", timeToLive=").append(timeToLive);
        sb.append(", fetchSize=").append(fetchSize);
        sb.append(", idempotent=").append(idempotent);
        sb.append(", outgoingPayLoad=").append(outgoingPayLoad);
        sb.append(", pagingState=").append(pagingState);
        sb.append(", retryPolicy=").append(retryPolicy);
        sb.append(", resultSetAsyncListeners=").append(resultSetAsyncListeners);
        sb.append(", rowAsyncListeners=").append(rowAsyncListeners);
        sb.append(", tracing=").append(tracing);
        sb.append(", schemaNameProvider=").append(schemaNameProvider);
        sb.append(", readTimeoutInMillis=").append(readTimeout);
        sb.append(", DMLResultsDisplaySize=").append(DMLResultsDisplaySize);
        sb.append('}');
        return sb.toString();
    }
}
