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

import java.util.*;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.Statement;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;

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
            PersistenceContextFactory contextFactory, EntityState entityState, Object[] boundValues) {
        super(entityClass, daoContext, configContext, statement, meta, contextFactory, entityState, boundValues);
    }

    /**
     * With provided paging state
     *
     * @return current typed query
     */
    public TypedQuery<T> withPagingState(PagingState pagingState) {
        super.pagingStateO = Optional.fromNullable(pagingState);
        return this;
    }

    /**
     * With provided paging state as String
     *
     * @return current typed query
     */
    public TypedQuery<T> withPagingState(String pagingState) {
        super.pagingStateO = Optional.fromNullable(PagingState.fromString(pagingState));
        return this;
    }

    /**
     * With provided paging state as byte array
     *
     * @return current typed query
     */
    public TypedQuery<T> withPagingState(byte[] pagingState) {
        super.pagingStateO = Optional.fromNullable(PagingState.fromBytes(pagingState));
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
        return asyncUtils.buildInterruptible(asyncGetInternal()).getImmediately().getEntities();
    }

    /**
     * Executes the query and returns entities with the paging state
     * <p/>
     * Matching CQL rows are mapped to entities by reflection. All un-mapped
     * columns are ignored.
     * <p/>
     * The size of the list is equal or lesser than the number of matching CQL
     * row, because some null or empty rows are ignored and filtered out
     *
     * @return EntitiesWithPagingState<T> list of found entities or empty list with possibly null paging state
     *
     */
    public EntitiesWithPagingState<T> getWithPagingState() {
        log.debug("Get results for typed query '{}'", nativeStatementWrapper.getStatement());
        return asyncUtils.buildInterruptible(asyncGetInternal()).getImmediately().toEntitiesWithPagingState();
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
        return asyncUtils.buildInterruptible(asyncGetFirstInternal()).getImmediately();
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
