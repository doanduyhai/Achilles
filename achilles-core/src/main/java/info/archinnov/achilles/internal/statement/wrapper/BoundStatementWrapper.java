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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import info.archinnov.achilles.listener.CASResultListener;

public class BoundStatementWrapper extends AbstractStatementWrapper {

    private BoundStatement boundStatement;

    public BoundStatementWrapper(Class<?> entityClass, BoundStatement bs, Object[] values, ConsistencyLevel consistencyLevel, Optional<CASResultListener> casResultListener) {
        super(entityClass, values);
        super.casResultListener = casResultListener;
        this.boundStatement = bs;
        this.boundStatement.setConsistencyLevel(consistencyLevel);
    }

    @Override
    public ResultSet execute(Session session) {
        logDMLStatement("");
        activateQueryTracing(boundStatement);
        ResultSet resultSet = session.execute(boundStatement);
        tracing(resultSet);
        checkForCASSuccess(boundStatement.preparedStatement().getQueryString(), resultSet);
        return resultSet;
    }

    @Override
    public BoundStatement getStatement() {
        return boundStatement;
    }

    @Override
    public void logDMLStatement(String indentation) {
        if (dmlLogger.isDebugEnabled()) {
            PreparedStatement ps = boundStatement.preparedStatement();
            String queryType = "Prepared statement";
            String queryString = ps.getQueryString();
            String consistencyLevel = boundStatement.getConsistencyLevel() == null ? "DEFAULT" : boundStatement
                    .getConsistencyLevel().name();
            writeDMLStatementLog(queryType, queryString, consistencyLevel, values);
        }
    }
}
