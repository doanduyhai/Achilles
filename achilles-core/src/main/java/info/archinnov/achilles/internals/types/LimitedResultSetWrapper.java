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
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

public class LimitedResultSetWrapper implements ResultSet {

    private final ResultSet delegate;

    public LimitedResultSetWrapper(ResultSet delegate) {
        this.delegate = delegate;
    }

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return delegate.getColumnDefinitions();
    }

    @Override
    public boolean isExhausted() {
        return delegate.isExhausted();
    }

    @Override
    public Row one() {
        throw new UnsupportedOperationException("You are not allowed to consume the ResultSet at this stage");
    }

    @Override
    public List<Row> all() {
        throw new UnsupportedOperationException("You are not allowed to consume the ResultSet at this stage");
    }

    @Override
    public Iterator<Row> iterator() {
        throw new UnsupportedOperationException("You are not allowed to consume the ResultSet at this stage");
    }

    @Override
    public int getAvailableWithoutFetching() {
        return delegate.getAvailableWithoutFetching();
    }

    @Override
    public boolean isFullyFetched() {
        return delegate.isFullyFetched();
    }

    @Override
    public ListenableFuture<ResultSet> fetchMoreResults() {
        throw new UnsupportedOperationException("You are not allowed to consume the ResultSet at this stage");
    }

    @Override
    public ExecutionInfo getExecutionInfo() {
        return delegate.getExecutionInfo();
    }

    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
        return delegate.getAllExecutionInfo();
    }

    @Override
    public boolean wasApplied() {
        return delegate.wasApplied();
    }
}
