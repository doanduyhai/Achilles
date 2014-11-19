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
package info.archinnov.achilles.query.cql;

import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.datastax.driver.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.persistence.operations.NativeQueryMapper;
import info.archinnov.achilles.internal.persistence.operations.TypedMapIterator;
import info.archinnov.achilles.internal.statement.wrapper.NativeQueryLog;
import info.archinnov.achilles.internal.statement.wrapper.NativeStatementWrapper;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.TypedMap;

/**
 * Class to wrap CQL3 native query
 *
 * <pre class="code"><code class="java">
 *
 *   String nativeQuery = "SELECT name,age_in_years FROM UserEntity WHERE id IN(?,?)";
 *   List<TypedMap> actual = manager.nativeQuery(nativeQuery,10L,11L).get();
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#native-query" target="_blank">Native query</a>
 */
public class NativeQuery {
    private static final Logger log = LoggerFactory.getLogger(NativeQuery.class);

    protected NativeStatementWrapper nativeStatementWrapper;

    private DaoContext daoContext;
    protected AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();
    protected NativeQueryMapper mapper = NativeQueryMapper.Singleton.INSTANCE.get();

    private ExecutorService executorService;


    public NativeQuery(DaoContext daoContext, ConfigurationContext configContext, Statement statement, Options options, Object... boundValues) {
        this.daoContext = daoContext;
        this.nativeStatementWrapper = new NativeStatementWrapper(NativeQueryLog.class, statement, boundValues, options.getLWTResultListener());
        this.executorService = configContext.getExecutorService();
    }

    /**
     * Return found rows. The list represents the number of returned rows The
     * map contains the (column name, column value) of each row. The map is
     * backed by a LinkedHashMap and thus preserves the columns order as they
     * were declared in the native query
     *
     * @return List<TypedMap>
     */
    public List<TypedMap> get() {
        log.debug("Get results for native query '{}'", nativeStatementWrapper.getStatement());
        return asyncGet().getImmediately();
    }

    /**
     * Return found rows asynchronously. The list represents the number of returned rows The
     * map contains the (column name, column value) of each row. The map is
     * backed by a LinkedHashMap and thus preserves the columns order as they
     * were declared in the native query
     *
     * @return AchillesFuture<List<TypedMap>>
     */
    public AchillesFuture<List<TypedMap>> asyncGet(FutureCallback<Object>... asyncListeners) {
        log.debug("Get results for native query '{}' asynchronously", nativeStatementWrapper.getStatement());

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);

        final ListenableFuture<List<Row>> futureRows = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROWS, executorService);

        Function<List<Row>, List<TypedMap>> rowsToTypedMaps = new Function<List<Row>, List<TypedMap>>() {
            @Override
            public List<TypedMap> apply(List<Row> rows) {
                return mapper.mapRows(rows);
            }
        };

        final ListenableFuture<List<TypedMap>> futureTypedMap = asyncUtils.transformFuture(futureRows, rowsToTypedMaps, executorService);

        asyncUtils.maybeAddAsyncListeners(futureTypedMap, asyncListeners, executorService);

        return asyncUtils.buildInterruptible(futureTypedMap);
    }

    /**
     * Return the first found row. The map contains the (column name, column
     * value) of each row. The map is backed by a LinkedHashMap and thus
     * preserves the columns order as they were declared in the native query
     *
     * @return TypedMap
     */
    public TypedMap first() {
        log.debug("Get first result for native query {}", nativeStatementWrapper.getStatement());
        return asyncFirst().getImmediately();
    }

    /**
     * Return the first found row asynchronously. The map contains the (column name, column
     * value) of each row. The map is backed by a LinkedHashMap and thus
     * preserves the columns order as they were declared in the native query
     *
     * @return AchillesFuture<TypedMap>Map
     */
    public AchillesFuture<TypedMap> asyncFirst(FutureCallback<Object>... asyncListeners) {
        log.debug("Get first result for native query '{}' asynchronously", nativeStatementWrapper.getStatement());
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);
        final ListenableFuture<List<Row>> futureRows = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROWS, executorService);

        Function<List<Row>, TypedMap> rowsToTypedMap = new Function<List<Row>, TypedMap>() {
            @Override
            public TypedMap apply(List<Row> rows) {
                List<TypedMap> result = mapper.mapRows(rows);
                if (result.isEmpty()) {
                    return null;
                } else {
                    return result.get(0);
                }
            }
        };
        final ListenableFuture<TypedMap> futureTypedMap = asyncUtils.transformFuture(futureRows, rowsToTypedMap, executorService);

        asyncUtils.maybeAddAsyncListeners(futureTypedMap, asyncListeners, executorService);

        return asyncUtils.buildInterruptible(futureTypedMap);
    }

    /**
     * Execute statement without returning result. Useful for
     * INSERT/UPDATE/DELETE and DDL statements
     */
    public void execute() {
        log.debug("Execute native query '{}'", nativeStatementWrapper.getStatement());
        asyncExecute().getImmediately();
    }

    /**
     * Execute statement asynchronously without returning result. Useful for
     * INSERT/UPDATE/DELETE and DDL statements
     */
    public AchillesFuture<Empty> asyncExecute(FutureCallback<Object>... asyncListeners) {
        log.debug("Execute native query '{}' asynchronously", nativeStatementWrapper.getStatement());
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);
        final ListenableFuture<Empty> futureEmpty = asyncUtils.transformFutureToEmpty(resultSetFuture, executorService);

        asyncUtils.maybeAddAsyncListeners(futureEmpty, asyncListeners, executorService);
        return asyncUtils.buildInterruptible(futureEmpty);
    }

    /**
     * Return an iterator of {@link info.archinnov.achilles.type.TypedMap} instance. Each instance represents a CQL row
     * @return Iterator<TypedMap>
     */
    public Iterator<TypedMap> iterator() {
        log.debug("Execute native query {} and return iterator", nativeStatementWrapper.getStatement());
        final ListenableFuture<ResultSet> future = daoContext.execute(nativeStatementWrapper);
        return new TypedMapIterator(asyncUtils.buildInterruptible(future).getImmediately().iterator());
    }

    /**
     * Return an asynchronous iterator of {@link info.archinnov.achilles.type.TypedMap} instance. Each instance represents a CQL row
     * @return Iterator<TypedMap>
     */
    public AchillesFuture<Iterator<TypedMap>> asyncIterator() {
        log.debug("Execute native query {} and return iterator", nativeStatementWrapper.getStatement());
        final ListenableFuture<ResultSet> futureResultSet = daoContext.execute(nativeStatementWrapper);

        final Function<ResultSet, Iterator<TypedMap>> toTypedMap = new Function<ResultSet, Iterator<TypedMap>>() {
            @Override
            public Iterator<TypedMap> apply(ResultSet resultSet) {
                return new TypedMapIterator(resultSet.iterator());
            }
        };

        final ListenableFuture<Iterator<TypedMap>> futureTypedMapIterator = asyncUtils.transformFuture(futureResultSet, toTypedMap, executorService);
        return asyncUtils.buildInterruptible(futureTypedMapIterator);
    }

}
