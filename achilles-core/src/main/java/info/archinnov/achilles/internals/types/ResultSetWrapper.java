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
import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

public class ResultSetWrapper implements ResultSet {

    private final ResultSet delegate;
    private final LinkedList<Row> values = new LinkedList<>();

    public ResultSetWrapper(ResultSet delegate) {
        this.delegate = delegate;
    }

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return delegate.getColumnDefinitions();
    }

    @Override
    public boolean isExhausted() {
        return values.size() == 0 && delegate.isExhausted();
    }

    public Row peek() {
        final Row row = delegate.one();
        values.add(row);
        return row;
    }

    @Override
    public Row one() {
        if (values.size() > 0) {
            return values.poll();
        } else {
            return delegate.one();
        }
    }

    @Override
    public List<Row> all() {
        values.addAll(delegate.all());
        return values;
    }

    @Override
    public Iterator<Row> iterator() {
        return new ResultLoggingIteratorWrapper<>(values, delegate.iterator());
    }

    @Override
    public int getAvailableWithoutFetching() {
        return values.size() + delegate.getAvailableWithoutFetching();
    }

    @Override
    public boolean isFullyFetched() {
        return delegate.isFullyFetched();
    }

    @Override
    public ListenableFuture<ResultSet> fetchMoreResults() {
        throw new UnsupportedOperationException("Not supported by Achilles ResultSetWrapper");
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

    @Override
    public String toString() {
        return delegate.toString();
    }
}
