/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.internals.types;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.Uninterruptibles;

import info.archinnov.achilles.internals.dsl.AsyncAware;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.statements.StatementWrapper;

public class JSONIteratorWrapper implements Iterator<String>, AsyncAware {

    private final Iterator<Row> delegate;
    private final StatementWrapper statementWrapper;
    private final CassandraOptions options;
    private ExecutionInfo executionInfo;

    public JSONIteratorWrapper(CompletableFuture<ResultSet> futureRS, StatementWrapper statementWrapper, CassandraOptions cassandraOptions) {
        this.statementWrapper = statementWrapper;
        this.options = cassandraOptions;
        try {
            this.delegate = Uninterruptibles.getUninterruptibly(futureRS
                    .thenApply(cassandraOptions::resultSetAsyncListener)
                    .thenApply(statementWrapper::logTrace)
                    .thenApply(rs -> {
                        JSONIteratorWrapper.this.executionInfo = rs.getExecutionInfo();
                        return rs;
                    })
                    .thenApply(rs -> rs.iterator()));
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    public ExecutionInfo getExecutionInfo() {
        return this.executionInfo;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public String next() {
        if (delegate.hasNext()) {
            final Row row = delegate.next();
            statementWrapper.logReturnedRow(row);
            options.rowAsyncListener(row);
            return row.getString("[json]");
        } else {
            return null;
        }
    }
}
