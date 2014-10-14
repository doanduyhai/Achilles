/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.internal.statement.wrapper;

import static com.datastax.driver.core.BatchStatement.Type.LOGGED;
import static com.datastax.driver.core.ColumnDefinitions.Definition;
import static info.archinnov.achilles.listener.CASResultListener.CASResult;
import static info.archinnov.achilles.listener.CASResultListener.CASResult.Operation;
import static info.archinnov.achilles.listener.CASResultListener.CASResult.Operation.INSERT;
import static info.archinnov.achilles.listener.CASResultListener.CASResult.Operation.UPDATE;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import info.archinnov.achilles.exception.AchillesLightWeightTransactionException;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.TraceRetrievalException;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.TypedMap;

public abstract class AbstractStatementWrapper {
    public static final EventComparator EVENT_TRACE_COMPARATOR = new EventComparator();
    public static final String ACHILLES_DML_STATEMENT = "ACHILLES_DML_STATEMENT";
    protected static final String IF_NOT_EXIST_CLAUSE = " IF NOT EXISTS";
    protected static final String IF_CLAUSE = " IF ";
    protected static final String CAS_RESULT_COLUMN = "[applied]";

    protected static final Logger dmlLogger = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);
    protected RowMethodInvoker invoker = RowMethodInvoker.Singleton.INSTANCE.get();

    protected Optional<CASResultListener> casResultListener = Optional.absent();

    protected Object[] values = new Object[] { };
    protected boolean traceQueryForEntity = false;
    protected boolean displayDMLForEntity = false;
    protected Logger entityLogger;

    protected AbstractStatementWrapper(Class<?> entityClass, Object[] values) {
        if (ArrayUtils.isNotEmpty(values)) {
            this.values = values;
        }
        if (entityClass != null && LoggerFactory.getLogger(entityClass) != null) {
            this.traceQueryForEntity = LoggerFactory.getLogger(entityClass).isTraceEnabled();
            this.displayDMLForEntity = LoggerFactory.getLogger(entityClass).isDebugEnabled();
            this.entityLogger = LoggerFactory.getLogger(entityClass);
        }

    }

    public Object[] getValues() {
        return values;
    }

    public abstract ResultSet execute(Session session);

    public abstract Statement getStatement();

    public abstract void logDMLStatement(String indentation);

    public static void writeDMLStartBatch(BatchStatement.Type batchType) {
        if (dmlLogger.isDebugEnabled()) {
            if (batchType == LOGGED) {
                dmlLogger.debug("");
                dmlLogger.debug("");
                dmlLogger.debug("****** BATCH LOGGED START ******");
                dmlLogger.debug("");
            } else {
                dmlLogger.debug("");
                dmlLogger.debug("");
                dmlLogger.debug("****** BATCH UNLOGGED START ******");
                dmlLogger.debug("");
            }
        }
    }

    public static void writeDMLEndBatch(BatchStatement.Type batchType, ConsistencyLevel consistencyLevel) {
        if (dmlLogger.isDebugEnabled()) {
            if (batchType == LOGGED) {
                dmlLogger.debug("");
                dmlLogger.debug("  ****** BATCH LOGGED END with CONSISTENCY LEVEL [{}] ******", consistencyLevel != null ? consistencyLevel : "DEFAULT");
                dmlLogger.debug("");
                dmlLogger.debug("");
            } else {
                dmlLogger.debug("");
                dmlLogger.debug("  ****** BATCH UNLOGGED END with CONSISTENCY LEVEL [{}] ******", consistencyLevel != null ? consistencyLevel : "DEFAULT");
                dmlLogger.debug("");
                dmlLogger.debug("");
            }
        }
    }

    protected void writeDMLStatementLog(String queryType, String queryString, String consistencyLevel, Object... values) {

        Logger actualLogger = displayDMLForEntity ? entityLogger : dmlLogger;

        actualLogger.debug("{} : [{}] with CONSISTENCY LEVEL [{}]", queryType, queryString, consistencyLevel);

        if (ArrayUtils.isNotEmpty(values)) {
            actualLogger.debug("\t bound values : {}", Arrays.asList(values));
        }
    }

    protected boolean isCASInsert(String queryString) {
        return queryString.contains(IF_NOT_EXIST_CLAUSE);
    }

    protected boolean isCASOperation(String queryString) {
        return queryString.contains(IF_CLAUSE);
    }

    protected void checkForCASSuccess(String queryString, ResultSet resultSet) {

        if (isCASOperation(queryString)) {
            final Row casResult = resultSet.one();
            if (!casResult.getBool(CAS_RESULT_COLUMN)) {
                TreeMap<String, Object> currentValues = new TreeMap<>();
                for (Definition columnDef : casResult.getColumnDefinitions()) {
                    final String columnDefName = columnDef.getName();
                    final DataType dataType = columnDef.getType();
                    final DataType.Name name = dataType.getName();

                    Object columnValue;
                    switch (name) {
                        case LIST:
                            columnValue = casResult.getList(columnDefName, dataType.getTypeArguments().get(0).asJavaClass());
                            break;
                        case SET:
                            columnValue = casResult.getSet(columnDefName, dataType.getTypeArguments().get(0).asJavaClass());
                            break;
                        case MAP:
                            final List<DataType> typeArguments = dataType.getTypeArguments();
                            columnValue = casResult.getMap(columnDefName, typeArguments.get(0).asJavaClass(), typeArguments.get(1).asJavaClass());
                            break;
                        default:
                            columnValue = invoker.invokeOnRowForType(casResult, name.asJavaClass(), columnDefName);
                    }
                    currentValues.put(columnDefName, columnValue);
                }

                Operation operation = UPDATE;
                if (isCASInsert(queryString)) {
                    operation = INSERT;
                }
                notifyCASError(new CASResult(operation, TypedMap.fromMap(currentValues)));
            } else {
                notifyCASSuccess();
            }

        }
    }

    protected void notifyCASError(CASResult casResult) {
        if (casResultListener.isPresent()) {
            casResultListener.get().onCASError(casResult);
        } else {
            throw new AchillesLightWeightTransactionException(casResult);
        }
    }

    protected void notifyCASSuccess() {
        if (casResultListener.isPresent()) {
            casResultListener.get().onCASSuccess();
        }
    }

    protected Statement activateQueryTracing(Statement statement) {
        if (dmlLogger.isTraceEnabled() || traceQueryForEntity) {
            statement.enableTracing();
        }
        return statement;
    }

    protected void tracing(ResultSet resultSet) {
        if (dmlLogger.isTraceEnabled() || traceQueryForEntity) {
            Logger actualLogger = traceQueryForEntity ? entityLogger : dmlLogger;
            for (ExecutionInfo executionInfo : resultSet.getAllExecutionInfo()) {
                actualLogger.trace("Query tracing at host {} with achieved consistency level {} ", executionInfo.getQueriedHost(), executionInfo.getAchievedConsistencyLevel());
                actualLogger.trace("****************************");
                actualLogger.trace(String.format("%1$-80s | %2$-16s | %3$-24s | %4$-20s", "Description", "Source", "Source elapsed in micros", "Thread name"));
                try {
                    final List<QueryTrace.Event> events = new ArrayList<>(executionInfo.getQueryTrace().getEvents());
                    Collections.sort(events, EVENT_TRACE_COMPARATOR);
                    for (QueryTrace.Event event : events) {
                        final String formatted = String.format("%1$-80s | %2$-16s | %3$-24s | %4$-20s", event.getDescription(), event.getSource(), event.getSourceElapsedMicros(), event.getThreadName());
                        actualLogger.trace(formatted);
                    }
                } catch (TraceRetrievalException e) {
                    actualLogger.trace("Cannot retrieve asynchronous trace, reason: " + e.getMessage());
                }
                actualLogger.trace("****************************");
            }
        }
    }

    private static class EventComparator implements Comparator<QueryTrace.Event> {
        @Override
        public int compare(QueryTrace.Event event1, QueryTrace.Event event2) {
            return event1.getSource().toString().compareTo(event2.getSource().toString());
        }
    }
}
