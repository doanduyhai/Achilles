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
package info.archinnov.achilles.query.typed;

import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;

/**
 * Class to perform native Cassandra query asynchronously and let <strong>Achilles</strong> map back the result set into list of entities.
 * In this case <strong>Achilles</strong> simply plays the role of a plain object mapper
 * <pre class="code"><code class="java">
 *
 *   Statement statement = new SimpleStatement("SELECT name,age_in_years FROM users WHERE id IN(?,?)");
 *   AchillesFutures<List<UserEntity>> usersFuture = asyncManager.typedQuery(UserEntity.class, statement, 10L, 11L).get();
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#typed-query" target="_blank">Typed query</a>
 * @param <T> entity type
 */
public class AsyncTypedQuery<T> extends AbstractTypedQuery<T> {
    private static final Logger log = LoggerFactory.getLogger(AsyncTypedQuery.class);

    public AsyncTypedQuery(Class<T> entityClass, DaoContext daoContext, ConfigurationContext configContext, Statement statement, EntityMeta meta,
                           PersistenceContextFactory contextFactory, Object[] boundValues) {
        super(entityClass, daoContext, configContext, statement, meta, contextFactory, boundValues);
    }

    /**
     * Return proxified entities
     *
     * @return current typed query
     */
    public AsyncTypedQuery<T> withProxy() {
        super.createProxy = true;
        return this;
    }

    /**
     * Executes the query and returns entities asynchronously
     *
     * Matching CQL rows are mapped to entities by reflection. All un-mapped
     * columns are ignored.
     *
     * The size of the list is equal or lesser than the number of matching CQL
     * row, because some null or empty rows are ignored and filtered out
     *
     * @return AchillesFuture&lt;List&lt;T&gt;&gt; future of list of found entities or empty list
     *
     */
    public AchillesFuture<List<T>> get(FutureCallback<Object>... asyncListeners) {
       return super.asyncGetInternal(asyncListeners);
    }


    /**
     * Executes the query and returns first entity
     *
     * Matching CQL row is mapped to entity by reflection. All un-mapped columns
     * are ignored.
     *
     * @return AchillesFuture&lt;T&gt; first found entity or null
     *
     */
    public AchillesFuture<T> getFirst(FutureCallback<Object>... asyncListeners) {
        return super.asyncGetFirstInternal(asyncListeners);
    }

    /**
     * Executes the query and returns an iterator
     *
     * Matching CQL row is mapped to entity by reflection. All un-mapped columns
     * are ignored.
     *
     * @return an AchillesFuture of an iterator of entities
     *
     */
    public AchillesFuture<Iterator<T>> iterator() {
        log.debug("Get iterator for typed query '{}'", nativeStatementWrapper.getStatement());
        return asyncIteratorInternal(Optional.<Integer>absent(),NO_CALLBACKS);
    }

    /**
     * Executes the query and returns an iterator
     *
     * Matching CQL row is mapped to entity by reflection. All un-mapped columns
     * are ignored.
     *
     * @param fetchSize the fetch size for the iterator
     * @return an AchillesFuture of an iterator of entities
     *
     */
    public AchillesFuture<Iterator<T>> iterator(int fetchSize) {
        log.debug("Get iterator for typed query '{}'", nativeStatementWrapper.getStatement());
        return asyncIteratorInternal(Optional.fromNullable(fetchSize),NO_CALLBACKS);
    }



}
