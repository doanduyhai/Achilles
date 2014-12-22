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

import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.TypedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Class to wrap CQL native query asynchronously
 *
 * <pre class="code"><code class="java">
 *
 *   Statement statement = new SimpleStatement("SELECT name,age_in_years FROM UserEntity WHERE id IN(?,?)");
 *   AchillesFuture<List<TypedMap>> future = manager.nativeQuery(statement,10L,11L).get();
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#native-query" target="_blank">Native query</a>
 */
public class AsyncNativeQuery extends AbstractNativeQuery {
    private static final Logger log = LoggerFactory.getLogger(AsyncNativeQuery.class);


    public AsyncNativeQuery(DaoContext daoContext, ConfigurationContext configContext, Statement statement, Options options, Object... boundValues) {
        super(daoContext, configContext, statement, options, boundValues);
    }

    /**
     * Return found rows asynchronously. The list represents the number of returned rows The
     * map contains the (column name, column value) of each row. The map is
     * backed by a LinkedHashMap and thus preserves the columns order as they
     * were declared in the native query
     *
     * @return AchillesFuture&lt;List&lt;TypedMap&gt;&gt;
     */
    public AchillesFuture<List<TypedMap>> get(FutureCallback<Object>... asyncListeners) {
        return super.asyncGetInternal(asyncListeners);
    }

    /**
     * Return the first found row asynchronously. The map contains the (column name, column
     * value) of each row. The map is backed by a LinkedHashMap and thus
     * preserves the columns order as they were declared in the native query
     *
     * @return AchillesFuture&lt;TypedMap&gt;Map
     */
    public AchillesFuture<TypedMap> getFirst(FutureCallback<Object>... asyncListeners) {
        return super.asyncGetFirstInternal(asyncListeners);
    }

    /**
     * Execute statement asynchronously without returning result. Useful for
     * INSERT/UPDATE/DELETE and DDL statements
     */
    public AchillesFuture<Empty> execute(FutureCallback<Object>... asyncListeners) {
        return super.asyncExecuteInternal(asyncListeners);
    }

    /**
     * Return an asynchronous iterator of {@link info.archinnov.achilles.type.TypedMap} instance. Each instance represents a CQL row
     * @return Iterator&lt;TypedMap&gt;
     */
    public AchillesFuture<Iterator<TypedMap>> iterator(FutureCallback<Object>... asyncListeners) {
       return super.asyncIterator(Optional.<Integer>absent(), asyncListeners);
    }

    /**
     * Return an asynchronous iterator of {@link info.archinnov.achilles.type.TypedMap} instance. Each instance represents a CQL row
     *
     * @param fetchSize the fetch size to set for paging
     * @return Iterator&lt;TypedMap&gt;
     */
    public AchillesFuture<Iterator<TypedMap>> iterator(int fetchSize, FutureCallback<Object>... asyncListeners) {
        return super.asyncIterator(Optional.fromNullable(fetchSize), asyncListeners);
    }
}
