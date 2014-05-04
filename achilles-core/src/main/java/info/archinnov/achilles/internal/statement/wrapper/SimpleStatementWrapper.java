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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.base.Optional;
import info.archinnov.achilles.listener.CASResultListener;

public class SimpleStatementWrapper extends AbstractStatementWrapper {

    private SimpleStatement simpleStatement;

    public SimpleStatementWrapper(String query, Object[] values, Optional<CASResultListener> casResultListener) {
        super(values);
        super.casResultListener = casResultListener;
        this.simpleStatement = new SimpleStatement(query);
    }

    @Override
    public ResultSet execute(Session session) {
        logDMLStatement("");
        ResultSet resultSet = session.execute(simpleStatement.getQueryString(), values);
        checkForCASSuccess(simpleStatement.getQueryString(), resultSet);
        return resultSet;
    }

    @Override
    public SimpleStatement getStatement() {
        return simpleStatement;
    }

    @Override
    public void logDMLStatement(String indentation) {
        if (dmlLogger.isDebugEnabled()) {
            String queryType = "Simple statement";
            String queryString = simpleStatement.getQueryString();
            String consistencyLevel = simpleStatement.getConsistencyLevel() == null ? "DEFAULT" : simpleStatement
                    .getConsistencyLevel().name();
            writeDMLStatementLog(queryType, queryString, consistencyLevel, values);
        }
    }
}
