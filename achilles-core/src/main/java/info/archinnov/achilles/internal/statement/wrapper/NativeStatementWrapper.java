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

import java.util.concurrent.ExecutorService;

import com.datastax.driver.core.*;
import info.archinnov.achilles.internal.statement.StatementHelper;
import info.archinnov.achilles.listener.LWTResultListener;
import org.apache.commons.lang3.ArrayUtils;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

public class NativeStatementWrapper extends AbstractStatementWrapper {


    private Statement statement;

    public NativeStatementWrapper(Class<?> entityClass, Statement statement, Object[] values, Optional<LWTResultListener> LWTResultListener) {
        super(entityClass, values);
        this.statement = statement;
        super.lwtResultListener = LWTResultListener;
    }


    @Override
    public String getQueryString() {
        return StatementHelper.maybeGetQueryString(statement);
    }

    @Override
    public ListenableFuture<ResultSet> executeAsync(Session session, ExecutorService executorService) {
        activateQueryTracing();
        return super.executeAsyncInternal(session, this, executorService);
    }

    @Override
    public Statement getStatement() {
        if (statement instanceof RegularStatement) {
            return buildParameterizedStatement();
        } else {
            return statement;
        }
    }

    @Override
    public void logDMLStatement(String indentation) {
        if (dmlLogger.isDebugEnabled() || displayDMLForEntity) {
            String queryType = statement.getClass().getSimpleName();
            String queryString = getQueryString();
            String consistencyLevel = statement.getConsistencyLevel() == null ? "DEFAULT" : statement
                    .getConsistencyLevel().name();
            writeDMLStatementLog(queryType, queryString, consistencyLevel, values);
        }
    }

    public Statement buildParameterizedStatement() {
        if (statement instanceof RegularStatement) {
            final RegularStatement regularStatement = (RegularStatement) statement;
            if (ArrayUtils.isEmpty(regularStatement.getValues(ProtocolVersion.V2)) && ArrayUtils.isNotEmpty(values)) {
                final SimpleStatement statement = new SimpleStatement(getQueryString(), values);

                if (this.statement.getConsistencyLevel() != null) {
                    statement.setConsistencyLevel(this.statement.getConsistencyLevel());
                }

                if (this.statement.getSerialConsistencyLevel() != null) {
                    statement.setSerialConsistencyLevel(this.statement.getSerialConsistencyLevel());
                }
                return statement;
            } else {
                return statement;
            }
        } else {
            return statement;
        }

    }
}
