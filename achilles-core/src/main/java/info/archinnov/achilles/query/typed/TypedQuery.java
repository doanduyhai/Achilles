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

import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROW;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;

import java.util.*;
import java.util.concurrent.ExecutorService;

import com.datastax.driver.core.Statement;
import info.archinnov.achilles.iterator.AchillesIterator;
import info.archinnov.achilles.listener.LWTResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityMapper;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.statement.wrapper.NativeStatementWrapper;

/**
 * Class to perform native Cassandra query and let <strong>Achilles</strong> map back the result set into list of entities.
 * In this case <strong>Achilles</strong> simply plays the role of a plain object mapper
 * <pre class="code"><code class="java">
 *
 *   Statement statement = new SimpleStatement("SELECT name,age_in_years FROM users WHERE id IN(?,?)");
 *   List<UserEntity> users = manager.typedQuery(UserEntity.class, statement, 10L, 11L).get();
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#typed-query" target="_blank">Typed query</a>
 * @param <T> entity type
 */
public class TypedQuery<T> extends AbstractTypedQuery<T> {
    private static final Logger log = LoggerFactory.getLogger(TypedQuery.class);

    public TypedQuery(Class<T> entityClass, DaoContext daoContext, ConfigurationContext configContext, Statement statement, EntityMeta meta,
            PersistenceContextFactory contextFactory, Object[] boundValues) {
        super(entityClass, daoContext, configContext, statement, meta, contextFactory, boundValues);
    }

    /**
     * Return proxified entities
     *
     * @return current typed query
     */
    public TypedQuery<T> withProxy() {
        super.createProxy = true;
        return this;
    }

    /**
     * Executes the query and returns entities
     * <p/>
     * Matching CQL rows are mapped to entities by reflection. All un-mapped
     * columns are ignored.
     * <p/>
     * The size of the list is equal or lesser than the number of matching CQL
     * row, because some null or empty rows are ignored and filtered out
     *
     * @return List<T> list of found entities or empty list
     *
     */
    public List<T> get() {
        log.debug("Get results for typed query '{}'", nativeStatementWrapper.getStatement());
        return asyncGetInternal().getImmediately();
    }


    /**
     * Executes the query and returns first entity
     *
     * Matching CQL row is mapped to entity by reflection. All un-mapped columns
     * are ignored.
     *
     * @return T first found entity or null
     *
     */
    public T getFirst() {
        log.debug("Get first result for typed query '{}'", nativeStatementWrapper.getStatement());
        return asyncGetFirstInternal().getImmediately();
    }

    /**
     * Executes the query and returns an iterator
     *
     * Matching CQL row is mapped to entity by reflection. All un-mapped columns
     * are ignored.
     *
     * @return an iterator of entities
     *
     */
    public Iterator<T> iterator() {
        log.debug("Get iterator for typed query '{}'", nativeStatementWrapper.getStatement());
        return asyncIteratorInternal(Optional.<Integer>absent(),NO_CALLBACKS).getImmediately();
    }

    /**
     * Executes the query and returns an iterator
     *
     * Matching CQL row is mapped to entity by reflection. All un-mapped columns
     * are ignored.
     *
     * @param fetchSize the fetch size for the iterator
     * @return an iterator of entities
     *
     */
    public Iterator<T> iterator(int fetchSize) {
        log.debug("Get iterator for typed query '{}'", nativeStatementWrapper.getStatement());
        return asyncIteratorInternal(Optional.fromNullable(fetchSize),NO_CALLBACKS).getImmediately();
    }
}
