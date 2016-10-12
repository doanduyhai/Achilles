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

import static org.apache.commons.lang3.ArrayUtils.isEmpty;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.datastax.driver.core.*;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.statement.StatementHelper;
import info.archinnov.achilles.listener.LWTResultListener;

public class NativeStatementWrapper extends AbstractStatementWrapper {


    private final DaoContext daoContext;
    private Statement statement;
    private PagingState pagingState;

    private static final List<ProtocolVersion> SUPPORTED_NATIVE_PROTOCOLS = Arrays.asList(
            ProtocolVersion.V1,
            ProtocolVersion.V2,
            ProtocolVersion.V3,
            ProtocolVersion.V4);

    public NativeStatementWrapper(DaoContext daoContext, Class<?> entityClass, Statement statement, Object[] values, Optional<LWTResultListener> LWTResultListener) {
        super(entityClass, values);
        this.daoContext = daoContext;
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

        if (pagingState != null) {
            statement.setPagingState(pagingState);
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

    @Override
    public void releaseResources() {
        // no op
    }

    public Statement buildParameterizedStatement() {
        if (statement instanceof RegularStatement) {

            final ProtocolVersion protocolVersion = daoContext.getSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
            final CodecRegistry codecRegistry = daoContext.getSession().getCluster().getConfiguration().getCodecRegistry();
            final RegularStatement regularStatement = (RegularStatement) statement;
            boolean statementHasNoBoundValue = true;

            for (ProtocolVersion proto : SUPPORTED_NATIVE_PROTOCOLS) {
                statementHasNoBoundValue &= isEmpty(regularStatement.getValues(proto, codecRegistry));
            }

            if (statementHasNoBoundValue && isNotEmpty(values)) {
                final SimpleStatement statement = new SimpleStatement(getQueryString(), values);

                statement.setFetchSize(this.statement.getFetchSize());
                statement.setKeyspace(this.statement.getKeyspace());
                statement.setConsistencyLevel(this.statement.getConsistencyLevel());
                statement.setDefaultTimestamp(this.statement.getDefaultTimestamp());
                statement.setRetryPolicy(this.statement.getRetryPolicy());

                if (this.statement.getRoutingKey(protocolVersion, codecRegistry) != null) {
                    statement.setRoutingKey(this.statement.getRoutingKey(protocolVersion, codecRegistry));
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

    public void setPagingState(PagingState pagingState) {
        this.pagingState = pagingState;
    }
}
