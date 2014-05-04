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
import java.util.Arrays;
import java.util.TreeMap;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import info.archinnov.achilles.exception.AchillesCASException;
import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.TypedMap;

public abstract class AbstractStatementWrapper {
    public static final String ACHILLES_DML_STATEMENT = "ACHILLES_DML_STATEMENT";
    protected static final String IF_NOT_EXIST_CLAUSE = " IF NOT EXISTS";
    protected static final String IF_CLAUSE = " IF ";
    protected static final String CAS_RESULT_COLUMN = "[applied]";

    protected static final Logger dmlLogger = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);
    protected Object[] values = new Object[] { };

    protected RowMethodInvoker invoker = new RowMethodInvoker();

    protected Optional<CASResultListener> casResultListener = Optional.absent();

    protected AbstractStatementWrapper(Object[] values) {
        if (ArrayUtils.isNotEmpty(values))
            this.values = values;
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

    protected void writeDMLStatementLog(String queryType, String queryString, String consistencyLevel,
            Object... values) {

        dmlLogger.debug("{} : [{}] with CONSISTENCY LEVEL [{}]", queryType, queryString, consistencyLevel);

        if (ArrayUtils.isNotEmpty(values)) {
            dmlLogger.debug("\t bound values : {}", Arrays.asList(values));
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
                    final Object columnValue = invoker.invokeOnRowForType(casResult, columnDef.getType().asJavaClass(), columnDefName);
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
            throw new AchillesCASException(casResult);
        }
    }

    protected void notifyCASSuccess() {
        if (casResultListener.isPresent()) {
            casResultListener.get().onCASSuccess();
        }
    }
}
