/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.Uninterruptibles;

import info.archinnov.achilles.internals.options.Options;
import info.archinnov.achilles.internals.query.AsyncAware;
import info.archinnov.achilles.internals.query.raw.TypedMapAware;
import info.archinnov.achilles.internals.statements.StatementWrapper;
import info.archinnov.achilles.type.TypedMap;

public class TypedMapIteratorWrapper implements Iterator<TypedMap>, TypedMapAware, AsyncAware {

    private final Iterator<Row> delegate;
    private final StatementWrapper statementWrapper;
    private final Options options;

    public TypedMapIteratorWrapper(CompletableFuture<ResultSet> futureRS, StatementWrapper statementWrapper, Options options) {
        this.statementWrapper = statementWrapper;
        this.options = options;
        try {
            this.delegate = Uninterruptibles.getUninterruptibly(futureRS
                    .thenApply(options::resultSetAsyncListener)
                    .thenApply(statementWrapper::logTrace)
                    .thenApply(rs -> rs.iterator()));
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public TypedMap next() {
        if (delegate.hasNext()) {
            final Row row = delegate.next();
            statementWrapper.logReturnedRow(row);
            options.rowAsyncListener(row);
            return mapRowToTypedMap(row);
        } else {
            return null;
        }
    }
}
