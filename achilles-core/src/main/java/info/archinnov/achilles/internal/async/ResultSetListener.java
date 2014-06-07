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

package info.archinnov.achilles.internal.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;

public class ResultSetListener implements FutureCallback<ResultSet> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultSetListener.class);

    private AbstractStatementWrapper statementWrapper;

    public ResultSetListener(AbstractStatementWrapper statementWrapper) {
        this.statementWrapper = statementWrapper;
    }

    @Override
    public void onSuccess(ResultSet resultSet) {
        LOGGER.trace("Apply Logging and maybe invoke CAS listener for resultSet {} and query '{}'", resultSet.toString(), statementWrapper.getQueryString());
        statementWrapper.logDMLStatement("");
        statementWrapper.tracing(resultSet);
        statementWrapper.checkForCASSuccess(resultSet);
    }

    @Override
    public void onFailure(Throwable t) {
        LOGGER.error(t.getMessage());
    }
}
