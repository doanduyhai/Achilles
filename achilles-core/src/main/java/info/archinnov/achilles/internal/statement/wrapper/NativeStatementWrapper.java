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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.datastax.driver.core.*;
import info.archinnov.achilles.internal.statement.StatementHelper;
import info.archinnov.achilles.listener.LWTResultListener;
import org.apache.commons.lang3.ArrayUtils;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import static org.apache.commons.lang3.ArrayUtils.isEmpty;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;

public class NativeStatementWrapper extends AbstractStatementWrapper {


    private Statement statement;

    private static final List<ProtocolVersion> SUPPORTED_NATIVE_PROTOCOLS = Arrays.asList(ProtocolVersion.V1, ProtocolVersion.V2);

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
            this.statement = buildParameterizedStatement();
        }

        return statement;
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
            boolean statementHasNoBoundValue = true;

            for (ProtocolVersion protocolVersion : SUPPORTED_NATIVE_PROTOCOLS) {
                statementHasNoBoundValue &= isEmpty(regularStatement.getValues(protocolVersion));
            }

            if (statementHasNoBoundValue && isNotEmpty(values)) {
                final SimpleStatement statement = new SimpleStatement(getQueryString(), values);

                statement.setFetchSize(this.statement.getFetchSize());
                statement.setKeyspace(this.statement.getKeyspace());
                statement.setConsistencyLevel(this.statement.getConsistencyLevel());
                statement.setDefaultTimestamp(this.statement.getDefaultTimestamp());
                statement.setRetryPolicy(this.statement.getRetryPolicy());

                if (this.statement.getRoutingKey() != null) {
                    statement.setRoutingKey(this.statement.getRoutingKey());
                }

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
