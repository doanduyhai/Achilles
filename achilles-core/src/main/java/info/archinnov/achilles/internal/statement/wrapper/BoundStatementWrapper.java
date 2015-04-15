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

import static com.google.common.base.Suppliers.compose;
import static com.google.common.base.Suppliers.memoize;
import java.util.concurrent.ExecutorService;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.listener.LWTResultListener;

public class BoundStatementWrapper extends AbstractStatementWrapper {

    private Supplier<BoundStatement> boundStatement;

    public BoundStatementWrapper(Class<?> entityClass, Supplier<BoundStatement> bs, Object[] values, ConsistencyLevel consistencyLevel,
            Optional<LWTResultListener> lwtResultListener, Optional<ConsistencyLevel> serialConsistencyLevel) {
        super(entityClass, values);
        super.lwtResultListener = lwtResultListener;
        this.boundStatement = memoize(compose(getFunc(consistencyLevel, serialConsistencyLevel), bs));
    }

    @Override
    public ListenableFuture<ResultSet> executeAsync(Session session, ExecutorService executorService) {
        activateQueryTracing();
        return super.executeAsyncInternal(session, this, executorService);
    }

    @Override
    public BoundStatement getStatement() {
        return boundStatement.get();
    }

    @Override
    public String getQueryString() {
        return boundStatement.get().preparedStatement().getQueryString();
    }

    @Override
    public void logDMLStatement(String indentation) {
        if (dmlLogger.isDebugEnabled() || displayDMLForEntity) {
            BoundStatement boundStatement = this.boundStatement.get();
            PreparedStatement ps = boundStatement.preparedStatement();
            String queryType = "Bound statement";
            String queryString = ps.getQueryString();
            String consistencyLevel = boundStatement.getConsistencyLevel() == null ? "DEFAULT" : boundStatement
                    .getConsistencyLevel().name();
            writeDMLStatementLog(queryType, queryString, consistencyLevel, values);
        }
    }

    @Override
    public void releaseResources() {
        values = null;
    }

    private Function<BoundStatement, BoundStatement> getFunc(final ConsistencyLevel consistencyLevel, final Optional<ConsistencyLevel> serialConsistencyLevel) {
        return new Function<BoundStatement, BoundStatement>() {
            @Override
            public BoundStatement apply(BoundStatement statement) {
                statement.setConsistencyLevel(consistencyLevel);
                if(serialConsistencyLevel.isPresent()) {
                    statement.setSerialConsistencyLevel(serialConsistencyLevel.get());
                }
                return statement;
            }
        };
    }
}
