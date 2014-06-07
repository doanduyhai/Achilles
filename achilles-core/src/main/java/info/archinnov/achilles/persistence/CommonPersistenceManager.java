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

import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.type.Options;

public class CommonPersistenceManager extends AbstractPersistenceManager {

    private static final Logger log = LoggerFactory.getLogger(CommonPersistenceManager.class);

    protected CommonPersistenceManager(Map<Class<?>, EntityMeta> entityMetaMap, PersistenceContextFactory contextFactory, DaoContext daoContext, ConfigurationContext configContext) {
        super(entityMetaMap, contextFactory, daoContext, configContext);
    }


    /**
     * Insert an entity.
     *
     *  <pre class="code"><code class="java">
     *      // Persist
     *      MyEntity managedEntity = manager.insert(myEntity);
     *  </code></pre>
     *
     * @param entity
     *            Entity to be inserted
     * @return proxified entity
     */
    public <T> T insert(T entity) {
        log.debug("Inserting entity '{}'", entity);
        return super.asyncInsert(entity, noOptions()).getImmediately();
    }

    /**
     * Insert an entity with the given options.
     *
     *  <pre class="code"><code class="java">
     *      // Persist
     *      MyEntity managedEntity = manager.insert(myEntity, OptionsBuilder.withTtl(3600));
     *  </code></pre>
     *
     * @param entity
     *            Entity to be inserted
     * @param options
     *            options
     * @return proxified entity
     */
    public <T> T insert(final T entity, Options options) {
        log.debug("Inserting entity '{}' with options {} ", entity, options);
        return super.asyncInsert(entity, options).getImmediately();
    }

    /**
     * Update a "managed" entity
     *
     *  <pre class="code"><code class="java">
     *      User managedUser = manager.find(User.class,1L);
     *      user.setFirstname("DuyHai");
     *
     *      manager.update(user);
     *  </code></pre>
     *
     * @param entity
     *            Managed entity to be updated
     */
    public void update(Object entity) {
        log.debug("Updating entity '{}'", proxifier.getRealObject(entity));
        super.asyncUpdate(entity, noOptions()).getImmediately();
    }

    /**
     * Update a "managed" entity with options
     *
     *  <pre class="code"><code class="java">
     *      User managedUser = manager.find(User.class,1L);
     *      user.setFirstname("DuyHai");
     *
     *      manager.update(user, OptionsBuilder.withTtl(10));
     *  </code></pre>
     *
     * @param entity
     *            Managed entity to be updated
     * @param options
     *            options
     */
    public void update(Object entity, Options options) {
        log.debug("Updating entity '{}' with options {} ", proxifier.getRealObject(entity), options);
        super.asyncUpdate(entity, options).getImmediately();
    }


    /**
     * Insert a "transient" entity or update a "managed" entity.
     *
     * Shorthand to insert() or update()
     *
     * @param entity
     *            Managed entity to be inserted/updated
     *
     * @return proxified entity
     */
    public <T> T insertOrUpdate(T entity) {
        log.debug("Inserting or updating entity '{}'", proxifier.getRealObject(entity));
        return this.asyncInsertOrUpdate(entity,noOptions()).getImmediately();
    }

    /**
     * Insert a "transient" entity or update a "managed" entity with options.
     *
     * Shorthand to insert() or update()
     *
     * @param entity
     *            Managed entity to be inserted/updated
     * @param options
     *            options
     */
    public <T> T insertOrUpdate(T entity, Options options) {
        log.debug("Inserting or updating entity '{}' with options {}", proxifier.getRealObject(entity), options);
        return this.asyncInsertOrUpdate(entity,options).getImmediately();
    }

    /**
     * Delete an entity.
     *
     *  <pre class="code"><code class="java">
     *      // Simple deletion
     *      User managedUser = manager.find(User.class,1L);
     *      manager.delete(managedUser);
     *  </code></pre>
     *
     * @param entity
     *            Entity to be deleted
     */
    public void delete(Object entity) {
        log.debug("Deleting entity '{}'", proxifier.getRealObject(entity));
        super.asyncDelete(entity, noOptions()).getImmediately();
    }

    /**
     * Delete an entity with the given options.
     *
     *  <pre class="code"><code class="java">
     *      // Deletion  with option
     *      User managedUser = manager.find(User.class,1L);
     *      manager.delete(managedUser, OptionsBuilder.withConsistency(QUORUM));
     *  </code></pre>
     *
     * @param entity
     *            Entity to be deleted
     * @param options
     *            options for consistency level and timestamp
     */
    public void delete(final Object entity, Options options) {
        log.debug("Deleting entity '{}' with options {}", proxifier.getRealObject(entity), options);
        super.asyncDelete(entity,options).getImmediately();
    }

    /**
     * Delete  an entity by its id.
     *
     *  <pre class="code"><code class="java">
     *      // Direct deletion  without read-before-write
     *      manager.deleteById(User.class,1L);
     *  </code></pre>
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     */
    public void deleteById(Class<?> entityClass, Object primaryKey) {
        log.debug("Deleting  entity of type '{}' by its id '{}'", entityClass, primaryKey);
        super.asyncDeleteById(entityClass, primaryKey, noOptions()).getImmediately();
    }

    /**
     * Delete  an entity by its id with the given options.
     *
     *  <pre class="code"><code class="java">
     *      // Direct deletion  without read-before-write
     *      manager.deleteById(User.class,1L,OptionsBuilder.withConsistency(QUORUM));
     *  </code></pre>
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     */
    public void deleteById(Class<?> entityClass, Object primaryKey, Options options) {
        log.debug("Deleting  entity of type '{}' by its id '{}'", entityClass, primaryKey);
        super.asyncDeleteById(entityClass, primaryKey, options).getImmediately();
    }
}
