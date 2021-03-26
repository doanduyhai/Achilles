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

package info.archinnov.achilles.internals.statements;

import static info.archinnov.achilles.internals.cql.TypeMapper.toJavaType;
import static info.archinnov.achilles.internals.utils.LoggerHelper.replaceByteBuffersByHexString;
import static java.lang.String.format;

import java.util.*;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.TraceRetrievalException;

import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.types.ResultSetWrapper;
import info.archinnov.achilles.logger.AchillesLoggers;

public interface StatementWrapper {
    Logger LOGGER = LoggerFactory.getLogger(StatementWrapper.class);

    EventComparator EVENT_TRACE_COMPARATOR = new EventComparator();
    Logger DML_LOGGER = LoggerFactory.getLogger(AchillesLoggers.ACHILLES_DML_STATEMENT);

    Object[] getBoundValues();

    BoundStatement getBoundStatement();

    void applyOptions(CassandraOptions cassandraOptions);

    void logDML();

    ResultSet logReturnResults(ResultSet resultSet, int maxDisplayedRows);

    Row logReturnedRow(Row row);

    ResultSet logTrace(ResultSet resultSet);

    default void writeDMLStatementLog(Logger actualLogger, UUID queryId, String queryString, ConsistencyLevel consistencyLevel, Object[] boundValues, Object[] encodedValues) {
        if (actualLogger.isDebugEnabled()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Writing DML log for query %s with id %s", queryString, queryId));
            }
            StringBuilder logBuilder = new StringBuilder("\n");
            logBuilder.append(String.format("Query ID %s : [%s] with CONSISTENCY LEVEL [%s]",
                    queryId.toString(), queryString, consistencyLevel));
            if (ArrayUtils.isNotEmpty(boundValues)) {
                logBuilder.append(String.format("\n\t Java bound values : %s", replaceByteBuffersByHexString(boundValues)));
                logBuilder.append(String.format("\n\t Encoded bound values : %s", replaceByteBuffersByHexString(encodedValues)));
            }
            actualLogger.debug(logBuilder.toString());
        }
    }

    default void logReturnedResultsInternal(Logger actualLogger, UUID queryId, ResultSetWrapper resultSet, int maxDisplayedRows) {
        if (maxDisplayedRows > 0) {
            final int availableWithoutFetching = resultSet.getAvailableWithoutFetching();
            StringBuilder results = new StringBuilder(format("Query ID %s results : \n", queryId));
            actualLogger.debug(resultSet.toString());
            for (int i = 0; i < Integer.min(availableWithoutFetching, maxDisplayedRows); i++) {
                final Row row = resultSet.peek();
                appendRowDataToBuilder(row, row.getColumnDefinitions().asList(), results);
            }
            actualLogger.debug(results.toString());
        }
    }

    default void logReturnedRowInternal(Logger actualLogger, UUID queryId, Row row) {
        StringBuilder results = new StringBuilder(format("Query ID %s row : \n", queryId));
        appendRowDataToBuilder(row, row.getColumnDefinitions().asList(), results);
        actualLogger.debug(results.toString());
    }

    default void appendRowDataToBuilder(Row row, List<ColumnDefinitions.Definition> columnsDef, StringBuilder builder) {
        StringJoiner joiner = new StringJoiner(", ", "\t", "\n");
        IntStream.range(0, columnsDef.size())
                .forEach(index -> {
                    final ColumnDefinitions.Definition def = columnsDef.get(index);
                    final Object value = extractValueFromRow(row, index, def.getType());
                    joiner.add(format("%s: %s", def.getName(), value));
                });
        builder.append(joiner.toString());
    }

    default Object extractValueFromRow(Row row, int index, DataType dataType) {
        final DataType.Name typeName = dataType.getName();
        switch (typeName) {
            case LIST:
            case SET:
            case MAP:
            case TUPLE:
            case CUSTOM:
                return row.getObject(index);
            default:
                return row.get(index, toJavaType(typeName));
        }
    }

    default void tracingInternal(Logger actualLogger, UUID queryId, ResultSet resultSet) {
        StringBuilder trace = new StringBuilder();
        if (actualLogger.isTraceEnabled()) {
            for (ExecutionInfo executionInfo : resultSet.getAllExecutionInfo()) {

                trace.append(format("\n\nTracing for Query ID %s at host %s with achieved consistency level %s \n", queryId.toString(), executionInfo.getQueriedHost(), executionInfo.getAchievedConsistencyLevel()));
                trace.append("****************************\n");
                trace.append(format("%1$-80s | %2$-16s | %3$-24s | %4$-20s\n", "Description", "Source", "Source elapsed in micros", "Thread name"));
                try {
                    final QueryTrace queryTrace = executionInfo.getQueryTrace();
                    if (queryTrace != null) {
                        final List<QueryTrace.Event> events = new ArrayList<>(queryTrace.getEvents());
                        Collections.sort(events, EVENT_TRACE_COMPARATOR);
                        for (QueryTrace.Event event : events) {
                            trace.append(format("%1$-80s | %2$-16s | %3$-24s | %4$-20s\n", event.getDescription(), event.getSource(), event.getSourceElapsedMicros(), event.getThreadName()));
                        }
                    }
                } catch (TraceRetrievalException e) {
                    final String queryString = getBoundStatement().preparedStatement().getQueryString();
                    trace.append(format(" ERROR: cannot retrieve trace for query %s because it may not be yet available", queryString));
                }
                trace.append("****************************\n\n");
            }

            actualLogger.trace(trace.toString());
        }
    }

    class EventComparator implements Comparator<QueryTrace.Event> {
        @Override
        public int compare(QueryTrace.Event event1, QueryTrace.Event event2) {
            return event1.getSource().toString().compareTo(event2.getSource().toString());
        }
    }
}
