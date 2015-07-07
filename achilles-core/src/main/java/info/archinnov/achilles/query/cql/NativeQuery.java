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

import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.persistence.operations.TypedMapIterator;
import info.archinnov.achilles.options.Options;
import info.archinnov.achilles.type.TypedMap;

/**
 * Class to wrap CQL native query
 *
 * <pre class="code"><code class="java">
 *
 *   Statement statement = new SimpleStatement("SELECT name,age_in_years FROM UserEntity WHERE id IN(?,?)");
 *   List<TypedMap> actual = manager.nativeQuery(statement,10L,11L).get();
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#native-query" target="_blank">Native query</a>
 */
public class NativeQuery extends AbstractNativeQuery {
    private static final Logger log = LoggerFactory.getLogger(NativeQuery.class);


    public NativeQuery(DaoContext daoContext, ConfigurationContext configContext, Statement statement, Options options, Object... boundValues) {
        super(daoContext, configContext, statement, options, boundValues);
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
        return asyncGetInternal().getImmediately();
    }


    /**
     * Return the first found row. The map contains the (column name, column
     * value) of each row. The map is backed by a LinkedHashMap and thus
     * preserves the columns order as they were declared in the native query
     *
     * @return TypedMap
     */
    public TypedMap getFirst() {
        log.debug("Get first result for native query {}", nativeStatementWrapper.getStatement());
        return asyncGetFirstInternal().getImmediately();
    }

    /**
     * Execute statement without returning result. Useful for
     * INSERT/UPDATE/DELETE and DDL statements
     */
    public void execute() {
        log.debug("Execute native query '{}'", nativeStatementWrapper.getStatement());
        asyncExecuteInternal().getImmediately();
    }

    /**
     * Return an iterator of {@link TypedMap} instance. Each instance represents a CQL row
     * @return Iterator&lt;TypedMap&gt;
     */
    public Iterator<TypedMap> iterator() {
        log.debug("Execute native query {} and return iterator", nativeStatementWrapper.getStatement());
        final ListenableFuture<ResultSet> future = daoContext.execute(nativeStatementWrapper);
        return new TypedMapIterator(asyncUtils.buildInterruptible(future).getImmediately().iterator());
    }

    /**
     * Return an iterator of {@link TypedMap} instance. Each instance represents a CQL row
     *
     * @param fetchSize the fetch size to set for paging
     * @return Iterator&lt;TypedMap&gt;
     */
    public Iterator<TypedMap> iterator(int fetchSize) {
        final Statement statement = nativeStatementWrapper.getStatement();
        log.debug("Execute native query {} and return iterator", statement);
        statement.setFetchSize(fetchSize);
        final ListenableFuture<ResultSet> future = daoContext.execute(nativeStatementWrapper);
        return new TypedMapIterator(asyncUtils.buildInterruptible(future).getImmediately().iterator());
    }
}
