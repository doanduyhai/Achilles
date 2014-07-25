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

import org.apache.commons.lang.ArrayUtils;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.base.Optional;
import info.archinnov.achilles.listener.CASResultListener;

public class NativeStatementWrapper extends AbstractStatementWrapper {


    private RegularStatement regularStatement;

    public NativeStatementWrapper(Class<?> entityClass, RegularStatement regularStatement, Object[] values, Optional<CASResultListener> casResultListener) {
        super(entityClass, values);
        this.regularStatement = regularStatement;
        super.casResultListener = casResultListener;
    }

    @Override
    public ResultSet execute(Session session) {
        logDMLStatement("");
        activateQueryTracing(regularStatement);
        ResultSet resultSet;
        if (ArrayUtils.isNotEmpty(super.values)) {
            resultSet = session.execute(regularStatement.getQueryString(), super.values);
        } else {
            resultSet = session.execute(regularStatement);
        }

        tracing(resultSet);
        checkForCASSuccess(regularStatement.getQueryString(), resultSet);
        return resultSet;
    }

    @Override
    public RegularStatement getStatement() {
        return regularStatement;
    }

    @Override
    public void logDMLStatement(String indentation) {
        if (dmlLogger.isDebugEnabled() || displayDMLForEntity) {
            String queryType = "Parameterized statement";
            String queryString = regularStatement.getQueryString();
            String consistencyLevel = regularStatement.getConsistencyLevel() == null ? "DEFAULT" : regularStatement
                    .getConsistencyLevel().name();
            writeDMLStatementLog(queryType, queryString, consistencyLevel, values);
        }
    }

    public SimpleStatement buildParameterizedStatement() {
        final SimpleStatement statement = new SimpleStatement(regularStatement.getQueryString(), values);

        if (regularStatement.getConsistencyLevel() != null) {
            statement.setConsistencyLevel(regularStatement.getConsistencyLevel());
        }

        if (regularStatement.getSerialConsistencyLevel() != null) {
            statement.setSerialConsistencyLevel(regularStatement.getSerialConsistencyLevel());
        }
        return statement;
    }
}
