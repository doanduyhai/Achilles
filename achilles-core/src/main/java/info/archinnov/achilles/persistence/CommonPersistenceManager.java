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
        return super.insert(entity, noOptions());
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
        return super.insert(entity, options);
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
        super.update(entity, noOptions());
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
        super.update(entity, options);
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
        return this.insertOrUpdate(entity,noOptions());
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
        entityValidator.validateEntity(entity, entityMetaMap);
        log.debug("Inserting or updating entity '{}' with options {}", proxifier.getRealObject(entity), options);

        if (proxifier.isProxy(entity)) {
            super.update(entity, options);
            return entity;
        } else {
            return super.insert(entity, options);
        }
    }

    /**
     * Remove an entity.
     *
     *  <pre class="code"><code class="java">
     *      // Simple removal
     *      User managedUser = manager.find(User.class,1L);
     *      manager.remove(managedUser);
     *  </code></pre>
     *
     * @param entity
     *            Entity to be removed
     */
    public void remove(Object entity) {
        log.debug("Removing entity '{}'", proxifier.getRealObject(entity));
        super.remove(entity, noOptions());
    }

    /**
     * Remove an entity with the given options.
     *
     *  <pre class="code"><code class="java">
     *      // Removal with option
     *      User managedUser = manager.find(User.class,1L);
     *      manager.remove(managedUser, OptionsBuilder.withConsistency(QUORUM));
     *  </code></pre>
     *
     * @param entity
     *            Entity to be removed
     * @param options
     *            options for consistency level and timestamp
     */
    public void remove(final Object entity, Options options) {
        log.debug("Removing entity '{}' with options {}", proxifier.getRealObject(entity), options);
        super.remove(entity,options);
    }

    /**
     * Remove an entity by its id.
     *
     *  <pre class="code"><code class="java">
     *      // Direct remove without read-before-write
     *      manager.removeById(User.class,1L);
     *  </code></pre>
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     */
    public void removeById(Class<?> entityClass, Object primaryKey) {
        log.debug("Removing entity of type '{}' by its id '{}'", entityClass, primaryKey);
        super.removeById(entityClass, primaryKey, noOptions());
    }

    /**
     * Remove an entity by its id with the given options.
     *
     *  <pre class="code"><code class="java">
     *      // Direct remove without read-before-write
     *      manager.removeById(User.class,1L,OptionsBuilder.withConsistency(QUORUM));
     *  </code></pre>
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     */
    public void removeById(Class<?> entityClass, Object primaryKey, Options options) {
        log.debug("Removing entity of type '{}' by its id '{}'", entityClass, primaryKey);
        super.removeById(entityClass, primaryKey, options);
    }
}
