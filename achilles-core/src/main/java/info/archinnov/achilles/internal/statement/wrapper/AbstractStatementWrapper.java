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

import static com.datastax.driver.core.ColumnDefinitions.Definition;
import static info.archinnov.achilles.listener.LWTResultListener.LWTResult;
import static info.archinnov.achilles.listener.LWTResultListener.LWTResult.Operation;
import static info.archinnov.achilles.listener.LWTResultListener.LWTResult.Operation.INSERT;
import static info.archinnov.achilles.listener.LWTResultListener.LWTResult.Operation.UPDATE;
import static java.lang.String.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import info.archinnov.achilles.exception.AchillesLightWeightTransactionException;
import java.util.concurrent.ExecutorService;

import info.archinnov.achilles.listener.LWTResultListener;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.TraceRetrievalException;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.TypedMap;

public abstract class AbstractStatementWrapper {
    public static final EventComparator EVENT_TRACE_COMPARATOR = new EventComparator();
    public static final String ACHILLES_DML_STATEMENT = "ACHILLES_DML_STATEMENT";
    protected static final String IF_NOT_EXIST_CLAUSE = " IF NOT EXISTS";
    protected static final String IF_CLAUSE = " IF ";
    protected static final String LWT_RESULT_COLUMN = "[applied]";

    protected static final Logger dmlLogger = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);
    protected RowMethodInvoker invoker = RowMethodInvoker.Singleton.INSTANCE.get();
    protected AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();

    protected Optional<LWTResultListener> LWTResultListener = Optional.absent();

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

    public abstract String getQueryString();

    public abstract ListenableFuture<ResultSet> executeAsync(Session session, ExecutorService executorService);

    public abstract Statement getStatement();

    public abstract void logDMLStatement(String indentation);

    protected ListenableFuture<ResultSet> executeAsyncInternal(Session session, AbstractStatementWrapper statementWrapper, ExecutorService executorService) {
        //Log DML statement BEFORE executing the query
        statementWrapper.logDMLStatement("");
        ResultSetFuture resultSetFuture = session.executeAsync(statementWrapper.getStatement());
        return asyncUtils.applyLoggingTracingAndCASCheck(resultSetFuture, statementWrapper, executorService);
    }

    public static void writeDMLStartBatch(BatchStatement.Type batchType) {
        if(dmlLogger.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder("\n\n");
            switch (batchType) {
                case LOGGED:
                    builder.append("****** BATCH LOGGED START ******\n");
                    break;
                case UNLOGGED:
                    builder.append("****** BATCH UNLOGGED START ******\n");
                    break;
                case COUNTER:
                    builder.append("****** BATCH COUNTER START ******\n");
                    break;
            }
            dmlLogger.debug(builder.toString());
        }
    }

    public static void writeDMLEndBatch(BatchStatement.Type batchType, ConsistencyLevel consistencyLevel) {
        if(dmlLogger.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder("\n");
            switch (batchType) {
                case LOGGED:
                    builder.append(format("  ****** BATCH LOGGED END  with CONSISTENCY LEVEL [%s] ******", consistencyLevel != null ? consistencyLevel : "DEFAULT"));
                    break;
                case UNLOGGED:
                    builder.append(format("  ****** BATCH UNLOGGED END with CONSISTENCY LEVEL [%s] ******", consistencyLevel != null ? consistencyLevel : "DEFAULT"));
                    break;
                case COUNTER:
                    builder.append(format("  ****** BATCH COUNTER END with CONSISTENCY LEVEL [%s] ******", consistencyLevel != null ? consistencyLevel : "DEFAULT"));
                    break;
            }
            builder.append("\n\n");
            dmlLogger.debug(builder.toString());
        }
    }

    protected void writeDMLStatementLog(String queryType, String queryString, String consistencyLevel, Object... values) {
        Logger actualLogger = displayDMLForEntity ? entityLogger : dmlLogger;

        if (actualLogger.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append(format("%s : [%s] with CONSISTENCY LEVEL [%s]", queryType, queryString, consistencyLevel));
            if (ArrayUtils.isNotEmpty(values)) {
                builder.append(format("\n\t bound values : %s", Arrays.asList(values)));
            }
            actualLogger.debug(builder.toString());
        }

    }

    protected boolean isLWTInsert(String queryString) {
        return queryString.contains(IF_NOT_EXIST_CLAUSE);
    }

    protected boolean isLWTOperation(String queryString) {
        return queryString.contains(IF_CLAUSE);
    }

    public void checkForLWTSuccess(ResultSet resultSet) {
        String queryString = this.getQueryString();
        if (isLWTOperation(queryString)) {
            final Row LWTResult = resultSet.one();
            if (LWTResult != null && !LWTResult.getBool(LWT_RESULT_COLUMN)) {
                TreeMap<String, Object> currentValues = new TreeMap<>();
                for (Definition columnDef : LWTResult.getColumnDefinitions()) {
                    final String columnDefName = columnDef.getName();
                    final DataType dataType = columnDef.getType();
                    final DataType.Name name = dataType.getName();

                    Object columnValue;
                    switch (name) {
                        case LIST:
                            columnValue = LWTResult.getList(columnDefName, dataType.getTypeArguments().get(0).asJavaClass());
                            break;
                        case SET:
                            columnValue = LWTResult.getSet(columnDefName, dataType.getTypeArguments().get(0).asJavaClass());
                            break;
                        case MAP:
                            final List<DataType> typeArguments = dataType.getTypeArguments();
                            columnValue = LWTResult.getMap(columnDefName, typeArguments.get(0).asJavaClass(), typeArguments.get(1).asJavaClass());
                            break;
                        default:
                            columnValue = invoker.invokeOnRowForType(LWTResult, name.asJavaClass(), columnDefName);
                    }
                    currentValues.put(columnDefName, columnValue);
                }

                Operation operation = UPDATE;
                if (isLWTInsert(queryString)) {
                    operation = INSERT;
                }
                notifyLWTError(new LWTResult(operation, TypedMap.fromMap(currentValues)));
            } else {
                notifyCASSuccess();
            }

        }
    }

    protected void notifyLWTError(LWTResult LWTResult) {
        if (LWTResultListener.isPresent()) {
            LWTResultListener.get().onError(LWTResult);
        } else {
            throw new AchillesLightWeightTransactionException(LWTResult);
        }
    }

    protected void notifyCASSuccess() {
        if (LWTResultListener.isPresent()) {
            LWTResultListener.get().onSuccess();
        }
    }

    public void activateQueryTracing() {
        if (isTracingEnabled()) {
            getStatement().enableTracing();
        }
    }

    public boolean isTracingEnabled() {
        return dmlLogger.isTraceEnabled() || traceQueryForEntity;
    }

    public void tracing(ResultSet resultSet) {
        if (isTracingEnabled()) {
            Logger actualLogger = traceQueryForEntity ? entityLogger : dmlLogger;
            for (ExecutionInfo executionInfo : resultSet.getAllExecutionInfo()) {
                StringBuilder builder = new StringBuilder();
                builder.append(format("Query tracing at host %s with achieved consistency level %s \n", executionInfo.getQueriedHost(), executionInfo.getAchievedConsistencyLevel()));
                builder.append("****************************\n");
                builder.append(format("%1$-80s | %2$-16s | %3$-24s | %4$-20s\n", "Description", "Source", "Source elapsed in micros", "Thread name"));
                try {
                    final QueryTrace queryTrace = executionInfo.getQueryTrace();
                    if (queryTrace != null) {
                        final List<QueryTrace.Event> events = new ArrayList<>(queryTrace.getEvents());
                        Collections.sort(events, EVENT_TRACE_COMPARATOR);
                        for (QueryTrace.Event event : events) {
                            builder.append(format("%1$-80s | %2$-16s | %3$-24s | %4$-20s\n", event.getDescription(), event.getSource(), event.getSourceElapsedMicros(), event.getThreadName()));
                        }
                    }
                } catch (TraceRetrievalException e) {
                    builder.append(format(" ERROR: cannot retrieve trace for query %s because it may not be yet available\n", getQueryString()));
                }
                builder.append("****************************\n");
                actualLogger.trace(builder.toString());
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
