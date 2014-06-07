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

package info.archinnov.achilles.internal.context.facade;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;

public interface DaoOperations extends PersistentStateHolder {

    public void pushStatement(AbstractStatementWrapper statementWrapper);

    public void pushCounterStatement(AbstractStatementWrapper statementWrapper);

    public ListenableFuture<ResultSet> executeImmediate(AbstractStatementWrapper bsWrapper);

}
