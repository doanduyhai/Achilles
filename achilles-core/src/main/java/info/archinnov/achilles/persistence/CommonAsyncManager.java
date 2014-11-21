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

package info.archinnov.achilles.persistence;

import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.type.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Future;

import static info.archinnov.achilles.type.OptionsBuilder.noOptions;

public class CommonAsyncManager extends AbstractPersistenceManager {

    private static final Logger log = LoggerFactory.getLogger(CommonAsyncManager.class);

    protected CommonAsyncManager(Map<Class<?>, EntityMeta> entityMetaMap, PersistenceContextFactory contextFactory, DaoContext daoContext, ConfigurationContext configContext) {
        super(entityMetaMap, contextFactory, daoContext, configContext);
    }


    /**
     * Insert an entity asynchronously.
     *
     *  <pre class="code"><code class="java">
     *      // Insert
     *      Future<MyEntity> managedEntityFuture = manager.insert(myEntity);
     *  </code></pre>
     *
     * @param entity
     *            Entity to be inserted
     * @return future of managed entity
     *
     */
    public <T> AchillesFuture<T> insert(T entity) {
        log.debug("Inserting asynchronously entity '{}'", entity);
        return super.asyncInsert(entity, noOptions());
    }

    /**
     * Insert an entity asynchronously with the given options.
     *
     *  <pre class="code"><code class="java">
     *      // Insert
     *      Future<MyEntity> managedEntityFuture = asyncManager.insert(myEntity, OptionsBuilder.withTtl(3600));
     *  </code></pre>
     *
     * @param entity
     *            Entity to be inserted
     * @param options
     *            options
     * @return future of managed entity
     */
    public <T> AchillesFuture<T> insert(final T entity, Options options) {
        log.debug("Inserting asynchronously entity '{}' with options {} ", entity, options);
        return super.asyncInsert(entity, options);
    }

    /**
     * Update asynchronously a "managed" entity
     *
     *  <pre class="code"><code class="java">
     *      User managedUser = asyncManager.find(User.class,1L).get();
     *      user.setFirstname("DuyHai");
     *
     *      Future<User> userFuture = asyncManager.update(user);
     *  </code></pre>
     *
     * @param entity
     *            Managed entity to be updated
     *
     * @return future of managed entity
     */
    public <T> AchillesFuture<T> update(T entity) {
        log.debug("Updating asynchronously entity '{}'", proxifier.getRealObject(entity));
        return super.asyncUpdate(entity, noOptions());
    }

    /**
     * Update asynchronously a "managed" entity with options
     *
     *  <pre class="code"><code class="java">
     *      User managedUser = asyncManager.find(User.class,1L).get();
     *      user.setFirstname("DuyHai");
     *
     *      Future<User> userFuture = asyncManager.update(user, OptionsBuilder.withTtl(10));
     *  </code></pre>
     *
     * @param entity
     *            Managed entity to be updated
     * @param options
     *            options
     *
     * @return future of managed entity
     */
    public <T> AchillesFuture<T> update(T entity, Options options) {
        log.debug("Updating asynchronously entity '{}' with options {} ", proxifier.getRealObject(entity), options);
        return super.asyncUpdate(entity, options);
    }


    /**
     * Insert a "transient" entity or update a "managed" entity asynchronously.
     *
     * Shorthand to insert() or update()
     *
     * @param entity
     *            Managed entity to be inserted/updated
     *
     * @return future of managed entity
     */
    public <T> AchillesFuture<T> insertOrUpdate(T entity) {
        log.debug("Inserting or updating asynchronously entity '{}'", proxifier.getRealObject(entity));
        return this.asyncInsertOrUpdate(entity,noOptions());
    }

    /**
     * Insert a "transient" entity or update a "managed" entity asynchronously with options.
     *
     * Shorthand to insert() or update()
     *
     * @param entity
     *            Managed entity to be inserted/updated
     * @param options
     *            options
     * @return future of managed entity
     */
    public <T> AchillesFuture<T> insertOrUpdate(T entity, Options options) {
        log.debug("Inserting or updating asynchronously entity '{}' with options {}", proxifier.getRealObject(entity), options);
        return this.asyncInsertOrUpdate(entity,options);
    }

    /**
     * Delete an entity asynchronously.
     *
     *  <pre class="code"><code class="java">
     *      // Simple deletion
     *      User managedUser = asyncManager.find(User.class,1L).get();
     *      Future<User> userFuture = asyncManager.delete(managedUser);
     *  </code></pre>
     *
     * @param entity
     *            Entity to be deleted
     *
     * @return future of managed entity
     */
    public <T> AchillesFuture<T> delete(T entity) {
        log.debug("Removing asynchronously entity '{}'", proxifier.getRealObject(entity));
        return super.asyncDelete(entity, noOptions());
    }

    /**
     * Delete an entity asynchronously with the given options.
     *
     *  <pre class="code"><code class="java">
     *      // Deletion with option
     *      User managedUser = asyncManager.find(User.class,1L);
     *      Future<User> userFuture = asyncManager.delete(managedUser, OptionsBuilder.withConsistency(QUORUM));
     *  </code></pre>
     *
     * @param entity
     *            Entity to be deleted
     * @param options
     *            options for consistency level and timestamp
     *
     * @return future of managed entity
     */
    public <T> AchillesFuture<T> delete(final T entity, Options options) {
        log.debug("Removing asynchronously entity '{}' with options {}", proxifier.getRealObject(entity), options);
        return super.asyncDelete(entity,options);
    }

    /**
     * Delete asynchronously an entity by its id.
     * <strong>The returned future will yield an {@code info.archinnov.achilles.type.Empty}.INSTANCE</strong>
     *
     *  <pre class="code"><code class="java">
     *      // Direct deletion without read-before-write
     *      Future<Empty> emptyFuture = asyncManager.deleteById(User.class,1L);
     *  </code></pre>
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     * @return future of {@code info.archinnov.achilles.type.Empty}.INSTANCE
     */
    public AchillesFuture<Empty> deleteById(Class<?> entityClass, Object primaryKey) {
        log.debug("Deleting asynchronously entity of type '{}' by its id '{}'", entityClass, primaryKey);
        return super.asyncDeleteById(entityClass, primaryKey, noOptions());
    }

    /**
     * Delete asynchronously an entity by its id with the given options.
     * <strong>The returned future will yield an {@code info.archinnov.achilles.type.Empty}.INSTANCE</strong>
     *
     *  <pre class="code"><code class="java">
     *      // Direct deletion without read-before-write
     *      Future<Empty> emptyFuture = asyncManager.deleteById(User.class,1L,OptionsBuilder.withConsistency(QUORUM));
     *  </code></pre>
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     *
     * @return future of {@code info.archinnov.achilles.type.Empty}.INSTANCE
     */
    public AchillesFuture<Empty> deleteById(Class<?> entityClass, Object primaryKey, Options options) {
        log.debug("Removing asynchronously entity of type '{}' by its id '{}'", entityClass, primaryKey);
        return super.asyncDeleteById(entityClass, primaryKey, options);
    }
}
