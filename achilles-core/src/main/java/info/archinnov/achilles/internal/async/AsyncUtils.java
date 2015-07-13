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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.datastax.driver.core.ExecutionInfo;
import com.google.common.util.concurrent.MoreExecutors;
import info.archinnov.achilles.query.cql.TypedMapsWithPagingState;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.type.TypedMap;
import org.apache.commons.lang3.ArrayUtils;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.options.Options;

import javax.annotation.Nullable;

public class AsyncUtils {

    public static final Function<ResultSet, List<Row>> RESULTSET_TO_ROWS = new Function<ResultSet, List<Row>>() {
        @Override
        public List<Row> apply(ResultSet resultSet) {
            List<Row> rows = new ArrayList<>();
            if (resultSet != null) {
                final int availableWithoutFetching = resultSet.getAvailableWithoutFetching();
                for (int i = 0; i < availableWithoutFetching; i++) {
                    rows.add(resultSet.one());
                }
            }
            return rows;
        }
    };

    public static final Function<ResultSet, RowsWithExecutionInfo> RESULTSET_TO_ROWS_WITH_EXECUTION_INFO = new Function<ResultSet, RowsWithExecutionInfo>() {
        @Override
        public RowsWithExecutionInfo apply(ResultSet resultSet) {
            List<Row> rows = new ArrayList<>();
            ExecutionInfo info = null;
            if (resultSet != null) {
                final int availableWithoutFetching = resultSet.getAvailableWithoutFetching();
                for (int i = 0; i < availableWithoutFetching; i++) {
                    rows.add(resultSet.one());
                }
                info = resultSet.getExecutionInfo();
            }
            return new RowsWithExecutionInfo(rows, info);
        }
    };


    public static final Function<ResultSet, Row> RESULTSET_TO_ROW = new Function<ResultSet, Row>() {
        @Override
        public Row apply(ResultSet resultSet) {
            Row row = null;
            if (resultSet != null) {
                row = resultSet.one();
            }
            return row;
        }
    };

    public static final Function<ResultSet, Iterator<Row>> RESULTSET_TO_ITERATOR = new Function<ResultSet, Iterator<Row>>() {
        @Override
        public Iterator<Row> apply(ResultSet resultSet) {
            Iterator<Row> iterator = new ArrayList<Row>().iterator();
            if (resultSet != null) {
                iterator = resultSet.iterator();
            }
            return iterator;
        }
    };

    public void maybeAddAsyncListeners(ListenableFuture<?> listenableFuture, Options options, ExecutorService executorService) {
        if (options.hasAsyncListeners()) {
            for (FutureCallback<Object> callback : options.getAsyncListeners()) {
                Futures.addCallback(listenableFuture, callback, executorService);
            }
        }
    }

    public void maybeAddAsyncListeners(ListenableFuture<?> listenableFuture, Options options) {
        maybeAddAsyncListeners(listenableFuture, options, MoreExecutors.sameThreadExecutor());
    }

    public void maybeAddAsyncListeners(ListenableFuture<?> listenableFuture, FutureCallback<Object>[] asyncListeners, ExecutorService executorService) {
        if (ArrayUtils.isNotEmpty(asyncListeners)) {
            for (FutureCallback<Object> callback : asyncListeners) {
                Futures.addCallback(listenableFuture, callback, executorService);
            }
        }
    }

    public void maybeAddAsyncListeners(ListenableFuture<?> listenableFuture, FutureCallback<Object>[] asyncListeners) {
        maybeAddAsyncListeners(listenableFuture, asyncListeners, MoreExecutors.sameThreadExecutor());
    }

    public <T, V> ListenableFuture<T> transformFuture(ListenableFuture<V> from, Function<V, T> function, ExecutorService executorService) {
        return Futures.transform(from, function, executorService);
    }

    public <T, V> ListenableFuture<T> transformFuture(ListenableFuture<V> from, Function<V, T> function) {
        return Futures.transform(from, function, MoreExecutors.sameThreadExecutor());
    }

    public <V> ListenableFuture<Empty> transformFutureToEmpty(ListenableFuture<V> from, ExecutorService executorService) {
        Function<V, Empty> function = new Function<V, Empty>() {
            @Override
            public Empty apply(V input) {
                return Empty.INSTANCE;
            }
        };
        return Futures.transform(from, function, executorService);
    }

    public <T> AchillesFuture<T> buildInterruptible(ListenableFuture<T> listenableFuture) {
        return new AchillesFuture<>(listenableFuture);
    }

    public ListenableFuture<List<ResultSet>> mergeResultSetFutures(List<ListenableFuture<ResultSet>> resultSetFutures) {
        return Futures.allAsList(resultSetFutures);
    }

    public ListenableFuture<ResultSet> applyLoggingTracingAndCASCheck(ResultSetFuture resultSetFuture, final AbstractStatementWrapper statementWrapper, ExecutorService executorService) {

        Function<ResultSet, ResultSet> tracing = new Function<ResultSet, ResultSet>() {
            @Override
            public ResultSet apply(ResultSet resultSet) {
                statementWrapper.tracing(resultSet);
                return resultSet;
            }
        };

        Function<ResultSet, ResultSet> LWTCheck = new Function<ResultSet, ResultSet>() {
            @Override
            public ResultSet apply(ResultSet resultSet) {
                statementWrapper.checkForLWTSuccess(resultSet);
                return resultSet;
            }
        };

        final ListenableFuture<ResultSet> tracingApplied = Futures.transform(resultSetFuture, tracing, executorService);
        return Futures.transform(tracingApplied, LWTCheck);
    }

    public static enum Singleton {
        INSTANCE;

        private final AsyncUtils instance = new AsyncUtils();

        public AsyncUtils get() {
            return instance;
        }
    }
}
