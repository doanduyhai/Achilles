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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState.MANAGED;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState.NOT_MANAGED;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import static info.archinnov.achilles.type.OptionsBuilder.withConsistency;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.facade.PersistenceManagerOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.persistence.operations.EntityValidator;
import info.archinnov.achilles.internal.persistence.operations.OptionsValidator;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.query.cql.NativeQuery;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.query.typed.TypedQuery;
import info.archinnov.achilles.query.typed.TypedQueryValidator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.Options;

/**
 * <p>
 * <strong>Stateless</strong> object to manage entity persistence.
 * This class is totally <strong></strong>thread-safe</strong> and can be shared by many threads.
 * You should normally have only one instance of PersistenceMananger across the application
 *
 * <br/>
 * A PersistenceMananger is very cheap to create from an {@link info.archinnov.achilles.persistence.PersistenceManagerFactory}
 * </p>
 *
 * <p>
 *  <h3>I Persist transient entity</h3>
 *  <pre class="code"><code class="java">
 *      // Persist
 *      MyEntity managedEntity = manager.insert(myEntity);
 *  </code></pre>
 *
 *  <h3>II Update for modifications</h3>
 *  <pre class="code"><code class="java">
 *      User managedUser = manager.find(User.class,1L);
 *      user.setFirstname("DuyHai");
 *
 *      manager.update(user);
 *  </code></pre>
 *
 *  <h3>III Removing entities</h3>
 *  <pre class="code"><code class="java">
 *      // Simple removed
 *      User managedUser = manager.find(User.class,1L);
 *      manager.remove(managedUser);
 *
 *      // Direct remove without read-before-write
 *      manager.removeById(User.class,1L);
 *  </code></pre>
 *
 *  <h3>IV Loading entities</h3>
 *  <pre class="code"><code class="java">
 *      // Read data from Cassandra
 *      User managedUser = manager.find(User.class,1L);
 *  </code></pre>
 *
 *  <h3>V Creating proxy for update</h3>
 *  <pre class="code"><code class="java">
 *      // No data read from Cassandra
 *      User managedUser = manager.getProxy(User.class,1L);
 *      managedUser.setAge(30);
 *
 *      // Direct update, no read from Cassandra has been done
 *      manager.update(managedUser);
 *  </code></pre>
 *
 *  <h3>VI Reloading state for managed entities</h3>
 *  <pre class="code"><code class="java">
 *      // Read data from Cassandra
 *      User managedUser = manager.find(User.class,1L);
 *      ...
 *      // Perform some logic
 *
 *      // Reload data from Cassandra into the managed entity
 *      manager.refresh(managedUser);
 *  </code></pre>
 *
 *  <h3>VII Initializing lazy fields for managed entity</h3>
 *  <pre class="code"><code class="java">
 *      // Create a proxy
 *      User managedUser = manager.getProxy(User.class,1L);
 *      ...
 *      // Perform some logic
 *
 *      // Initialize all fields not yet loaded into the managed entity, including counter fields
 *      manager.initialize(managedUser);
 *  </code></pre>
 *
 *  <h3>VIII Removing proxy from managed entities</h3>
 *  <pre class="code"><code class="java">
 *      // Create proxy
 *      User managedUser = manager.getProxy(User.class,1L);
 *      ...
 *      // Perform some logic
 *
 *      // Removing proxy before passing it to client via serialization
 *      User transientUser = manager.removeProxy(managedUser);
 *  </code></pre>
 *
 *  <h3>IX Accessing native Session object</h3>
 *  <pre class="code"><code class="java">
 *      Session session = manager.getNativeSession();
 *      ...
 *
 *      // Issue simple CQL3 queries
 *      session.execute("UPDATE users SET age=:age WHERE id=:id",30,10);
 *  </code></pre>
 *
 *  <h3>X JSON serialization/deserialization</h3>
 *  <pre class="code"><code class="java">
 *      // Serialize an object to JSON using the registered or default object mapper
 *      String json = manager.serializeToJSON(myModel);
 *      ...
 *
 *      // Deserialize a JSON string into an object  the registered or default object mapper
 *      MyModel myModel = manager.deserializeFromJSON(json);
 *  </code></pre>
 *
 *  <h3>XI Initializing all lazy fields</h3>
 *  <pre class="code"><code class="java">
 *      // Create proxy
 *      User userProxy = manager.getProxy(User.class,1L);
 *      ...
 *      // Perform some logic
 *      ...
 *
 *      // Load all other lazy fields
 *      manager.initialize(userProxy);
 *  </code></pre>
 * </p>
 *
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Persistence-Manager-Operations" target="_blank">Persistence Manager operations</a>
 */
public class PersistenceManager {
    private static final Logger log = LoggerFactory.getLogger(PersistenceManager.class);

    protected Map<Class<?>, EntityMeta> entityMetaMap;
    protected ConfigurationContext configContext;
    protected PersistenceContextFactory contextFactory;

    protected EntityProxifier proxifier = new EntityProxifier();
    private EntityValidator entityValidator = new EntityValidator();
    private OptionsValidator optionsValidator = new OptionsValidator();
    private TypedQueryValidator typedQueryValidator = new TypedQueryValidator();

    private SliceQueryExecutor sliceQueryExecutor;

    protected DaoContext daoContext;

    protected PersistenceManager(Map<Class<?>, EntityMeta> entityMetaMap, //
            PersistenceContextFactory contextFactory, DaoContext daoContext, ConfigurationContext configContext) {
        this.entityMetaMap = entityMetaMap;
        this.configContext = configContext;
        this.daoContext = daoContext;
        this.contextFactory = contextFactory;
        this.sliceQueryExecutor = new SliceQueryExecutor(contextFactory, configContext, daoContext);
    }

    /**
     * Persist an entity.
     *
     *  <pre class="code"><code class="java">
     *      // Persist
     *      MyEntity managedEntity = manager.insert(myEntity);
     *  </code></pre>
     *
     * @param entity
     *            Entity to be persisted
     * @return proxified entity
     */
    public <T> T insert(T entity) {
        log.debug("Persisting entity '{}'", entity);
        return insert(entity, noOptions());
    }

    /**
     * Persist an entity with the given options.
     *
     *  <pre class="code"><code class="java">
     *      // Persist
     *      MyEntity managedEntity = manager.insert(myEntity, OptionsBuilder.withTtl(3600));
     *  </code></pre>
     *
     * @param entity
     *            Entity to be persisted
     * @param options
     *            options
     * @return proxified entity
     */
    public <T> T insert(final T entity, Options options) {
        if (log.isDebugEnabled()) {
            log.debug("Persisting entity '{}' with options {} ", entity, options);
        }

        entityValidator.validateEntity(entity, entityMetaMap);

        optionsValidator.validateOptionsForInsert(entity, entityMetaMap, options);
        proxifier.ensureNotProxy(entity);
        PersistenceManagerOperations context = initPersistenceContext(entity, options);
        return context.persist(entity);
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
        if (log.isDebugEnabled()) {
            log.debug("Updating entity '{}'", proxifier.getRealObject(entity));
        }
        update(entity, noOptions());
    }

    /**
     * Update a "managed" entity
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
        proxifier.ensureProxy(entity);
        Object realObject = proxifier.getRealObject(entity);
        if (log.isDebugEnabled()) {
            log.debug("Updating entity '{}' with options {} ", realObject, options);
        }
        entityValidator.validateEntity(realObject, entityMetaMap);
        optionsValidator.validateOptionsForUpdate(entity, entityMetaMap, options);
        PersistenceManagerOperations context = initPersistenceContext(realObject, options);
        context.update(entity);
    }


    /**
     * Insert a "transient" entity or update a "managed" entity.
     *
     * Shorthand to insert() or update()
     *
     * @param entity
     *            Managed entity to be updated
     */
    public void insertOrUpdate(Object entity) {
        entityValidator.validateEntity(entity, entityMetaMap);
        Object realObject = proxifier.getRealObject(entity);
        if (log.isDebugEnabled()) {
            log.debug("Inserting or updating entity '{}'", realObject);
        }

        if (proxifier.isProxy(entity)) {
            this.update(entity, noOptions());
        } else {
            this.insert(entity, noOptions());
        }
    }

    /**
     * Insert a "transient" entity or update a "managed" entity.
     *
     * Shorthand to insert() or update()
     *
     * @param entity
     *            Managed entity to be updated
     * @param options
     *            options
     */
    public void insertOrUpdate(Object entity, Options options) {
        entityValidator.validateEntity(entity, entityMetaMap);
        Object realObject = proxifier.getRealObject(entity);
        if (log.isDebugEnabled()) {
            log.debug("Inserting or updating entity '{}' with options {}", realObject, options);
        }

        if (proxifier.isProxy(entity)) {
            this.update(entity, options);
        } else {
            this.insert(entity, options);
        }
    }

    /**
     * Remove an entity.
     *
     *  <pre class="code"><code class="java">
     *      // Simple removed
     *      User managedUser = manager.find(User.class,1L);
     *      manager.remove(managedUser);
     *  </code></pre>
     *
     * @param entity
     *            Entity to be removed
     */
    public void remove(Object entity) {
        if (log.isDebugEnabled()) {
            log.debug("Removing entity '{}'", proxifier.getRealObject(entity));
        }
        remove(entity, noOptions());
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
        Validator.validateNotNull(entityClass, "The entity class should not be null for removal by id");
        Validator.validateNotNull(primaryKey, "The primary key should not be null for removal by id");
        if (log.isDebugEnabled()) {
            log.debug("Removing entity of type '{}' by its id '{}'", entityClass, primaryKey);
        }
        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, noOptions());
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        context.remove();
    }

    /**
     * Remove an entity with the given Consistency Level for write.
     *
     *  <pre class="code"><code class="java">
     *      // Simple removed
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
        Object realObject = proxifier.getRealObject(entity);
        if (log.isDebugEnabled()) {
            log.debug("Removing entity '{}' with options {}", realObject, options);
        }

        entityValidator.validateEntity(realObject, entityMetaMap);
        PersistenceManagerOperations context = initPersistenceContext(realObject, options);
        context.remove();
    }

    /**
     * Remove an entity by its id with the given Consistency Level for write.
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
        Validator.validateNotNull(entityClass, "The entity class should not be null for removal by id");
        Validator.validateNotNull(primaryKey, "The primary key should not be null for removal by id");
        if (log.isDebugEnabled()) {
            log.debug("Removing entity of type '{}' by its id '{}'", entityClass, primaryKey);
        }

        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, options);
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        context.remove();
    }

    /**
     * Find an entity.
     *
     *  <pre class="code"><code class="java">
     *      // Read data from Cassandra
     *      User managedUser = manager.find(User.class,1L);
     *  </code></pre>
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to load
     */
    public <T> T find(Class<T> entityClass, Object primaryKey) {
        log.debug("Find entity class '{}' with primary key {}", entityClass, primaryKey);
        T entity = find(entityClass, primaryKey, null);
        return entity;
    }

    /**
     * Find an entity with the given Consistency Level for read
     *
     *  <pre class="code"><code class="java">
     *      // Read data from Cassandra
     *      User managedUser = manager.find(User.class,1L,QUORUM);
     *  </code></pre>
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to load
     * @param readLevel
     *            Consistency Level for read
     */
    public <T> T find(final Class<T> entityClass, final Object primaryKey, ConsistencyLevel readLevel) {
        log.debug("Find entity class '{}' with primary key {} and read consistency level {}", entityClass, primaryKey,
                readLevel);
        Validator.validateNotNull(entityClass, "Entity class should not be null for find by id");
        Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null for find by id");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass),
                "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());
        Validator.validateTrue(entityMetaMap.containsKey(entityClass),
                "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());
        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, withConsistency(readLevel));
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        return context.find(entityClass);
    }

    /**
     * Create a proxy for the entity. An new empty entity will be created,
     * populated with the provided primary key and then proxified. This method
     * never returns null Use this method to perform direct update without
     * read-before-write
     *
     *  <pre class="code"><code class="java">
     *      // No data read from Cassandra
     *      User managedUser = manager.getProxy(User.class,1L);
     *  </code></pre>
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to initialize
     */
    public <T> T getProxy(Class<T> entityClass, Object primaryKey) {
        if (log.isDebugEnabled()) {
            log.debug("Get reference for entity class '{}' with primary key {}", entityClass, primaryKey);
        }

        return getProxy(entityClass, primaryKey, null);
    }

    /**
     * Create a proxy for the entity. An new empty entity will be created,
     * populated with the provided primary key and then proxified. This method
     * never returns null Use this method to perform direct update without
     * read-before-write
     *
     *  <pre class="code"><code class="java">
     *      // No data read from Cassandra
     *      User managedUser = manager.getProxy(User.class,1L,OptionsBuilder.withConsistency(QUORUM));
     *  </code></pre>
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to initialize
     * @param readLevel
     *            Consistency Level for read
     */
    public <T> T getProxy(final Class<T> entityClass, final Object primaryKey, ConsistencyLevel readLevel) {
        if (log.isDebugEnabled()) {
            log.debug("Get reference for entity class '{}' with primary key {} and read consistency level {}",
                    entityClass, primaryKey, readLevel);
        }

        Validator.validateNotNull(entityClass, "Entity class should not be null for get reference");
        Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null for get reference");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass),
                "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());

        Validator.validateTrue(entityMetaMap.containsKey(entityClass),
                "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());

        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, withConsistency(readLevel));
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        T entity = context.getProxy(entityClass);
        return entity;
    }

    /**
     * Refresh an entity.
     *
     *  <pre class="code"><code class="java">
     *      // Create a proxy
     *      User managedUser = manager.getProxy(User.class,1L);
     *      ...
     *      // Perform some logic
     *
     *      // Initialize all fields not yet loaded into the managed entity, including counter fields
     *      manager.initialize(managedUser);
     *  </code></pre>
     *
     * @param entity
     *            Entity to be refreshed
     */
    public void refresh(Object entity) throws AchillesStaleObjectStateException {
        if (log.isDebugEnabled()) {
            log.debug("Refreshing entity '{}'", proxifier.removeProxy(entity));
        }
        refresh(entity, null);
    }

    /**
     * Refresh an entity with the given Consistency Level for read.
     *
     *  <pre class="code"><code class="java">
     *      // Create a proxy
     *      User managedUser = manager.getProxy(User.class,1L);
     *      ...
     *      // Perform some logic
     *
     *      // Initialize all fields not yet loaded into the managed entity, including counter fields
     *      manager.initialize(managedUser, QUORUM);
     *  </code></pre>
     *
     * @param entity
     *            Entity to be refreshed
     * @param readLevel
     *            Consistency Level for read
     */
    public void refresh(final Object entity, ConsistencyLevel readLevel) throws AchillesStaleObjectStateException {
        if (log.isDebugEnabled()) {
            log.debug("Refreshing entity '{}' with read consistency level {}", proxifier.removeProxy(entity), readLevel);
        }

        proxifier.ensureProxy(entity);
        Object realObject = proxifier.getRealObject(entity);
        entityValidator.validateEntity(realObject, entityMetaMap);
        PersistenceManagerOperations context = initPersistenceContext(realObject, withConsistency(readLevel));
        context.refresh(entity);
    }

    /**
     * Initialize all lazy fields of a set of 'managed' entities
     *
     *  <pre class="code"><code class="java">
     *      // Create a proxy
     *      User userProxy = manager.getProxy(User.class,1L);
     *      ...
     *      // Perform some logic
     *
     *      // Initialize all fields not yet loaded into the managed entity, including counter fields
     *      manager.initialize(userProxy);
     *  </code></pre>
     *
     * Raise an IllegalStateException if an entity is not 'managed'
     *
     */
    public <T> T initialize(final T entity) {
        log.debug("Force lazy fields initialization for entity {}", entity);
        if (log.isDebugEnabled()) {
            log.debug("Force lazy fields initialization for entity {}", proxifier.removeProxy(entity));
        }
        proxifier.ensureProxy(entity);
        T realObject = proxifier.getRealObject(entity);
        PersistenceManagerOperations context = initPersistenceContext(realObject, noOptions());
        return context.initialize(entity);
    }



    /**
     * Initialize all lazy fields of a list of 'managed' entities
     *
     *  <pre class="code"><code class="java">
     *      // Create proxies
     *      User userProxy1 = manager.getProxy(User.class,1L);
     *      User userProxy2 = manager.getProxy(User.class,2L);
     *      ...
     *      // Perform some logic
     *      ...
     *
     *      // Initialize all fields not yet loaded into the managed entity, including counter fields
     *      manager.initialize(Sets.newHashSet(userProxy1, userProxy2));
     *  </code></pre>
     *
     * Raise an IllegalStateException if an entity is not 'managed'
     *
     */
    public <T> Set<T> initialize(final Set<T> entities) {
        log.debug("Force lazy fields initialization for entity set {}", entities);
        for (T entity : entities) {
            initialize(entity);
        }
        return entities;
    }

    /**
     * Initialize all lazy fields of a list of 'managed' entities
     *
     *  <pre class="code"><code class="java">
     *      // Create proxies
     *      User userProxy1 = manager.getProxy(User.class,1L);
     *      User userProxy2 = manager.getProxy(User.class,2L);
     *      ...
     *      // Perform some logic
     *      ...
     *
     *      // Initialize all fields not yet loaded into the managed entity, including counter fields
     *      manager.initialize(Arrays.asList(userProxy1, userProxy2));
     *  </code></pre>
     *
     * Raise an IllegalStateException if an entity is not 'managed'
     *
     */
    public <T> List<T> initialize(final List<T> entities) {
        log.debug("Force lazy fields initialization for entity set {}", entities);
        for (T entity : entities) {
            initialize(entity);
        }
        return entities;
    }

    /**
     * Shorthand for manager.removeProxy(manager.initialize(T entity))
     *
     */
    public <T> T initAndRemoveProxy(T entity) {
        return removeProxy(initialize(entity));
    }

    /**
     * Shorthand for manager.removeProxy(manager.initialize(Set<T> entities))
     *
     */
    public <T> Set<T> initAndRemoveProxy(Set<T> entities) {
        return removeProxy(initialize(entities));
    }

    /**
     * Shorthand for manager.removeProxy(manager.initialize(List<T> entities))
     *
     */
    public <T> List<T> initAndRemoveProxy(List<T> entities) {
        return removeProxy(initialize(entities));
    }

    /**
     * Remove the proxy of a 'managed' entity and return the underlying "raw"
     * entity
     * <br/>
     * <br/>
     * If the argument is not a proxy objet, return itself <br/>
     * Else, return the target object behind the proxy
     *
     *  <pre class="code"><code class="java">
     *      // Create proxy
     *      User managedUser = manager.getProxy(User.class,1L);
     *      ...
     *      // Perform some logic
     *
     *      // Removing proxy before passing it to client via serialization
     *      User transientUser = manager.removeProxy(managedUser);
     *  </code></pre>
     *
     * @param proxy
     * @return real object
     */
    public <T> T removeProxy(T proxy) {
        log.debug("Removing proxy for entity {}", proxy);

        T realObject = proxifier.removeProxy(proxy);

        return realObject;
    }

    /**
     * Remove the proxy of a list of 'managed' entities and return the
     * underlying "raw" entities
     *
     * See {@link #removeProxy}
     *
     * @param proxies
     *            list of proxified entity
     * @return real object list
     */
    public <T> List<T> removeProxy(List<T> proxies) {
        log.debug("Removing proxy for a list of entities {}", proxies);

        return proxifier.removeProxy(proxies);
    }

    /**
     * Remove the proxy of a set of 'managed' entities return the underlying
     * "raw" entities
     *
     * See {@link #removeProxy}
     *
     * @param proxies
     *            set of proxified entities
     * @return real object set
     */
    public <T> Set<T> removeProxy(Set<T> proxies) {
        log.debug("Removing proxy for a set of entities {}", proxies);

        return proxifier.removeProxy(proxies);
    }

    /**
     * Create a builder to start slice query DSL. The provided entity class <strong>must</strong> be:
     *
     * <ul>
     *     <li>a entity type managed by <strong>Achilles</strong></li>
     *     <li>a clustered entity, slicing is irrelevant for non-clustered entity</li>
     * </ul>
     *
     * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#slice-query" target="_blank">Slice query API</a>
     *
     * @param entityClass type of the clustered entity
     * @param <T>: type of the clustered entity
     * @return SliceQueryBuilder
     */
    public <T> SliceQueryBuilder<T> sliceQuery(Class<T> entityClass) {
        log.debug("Execute slice query for entity class {}", entityClass);
        EntityMeta meta = entityMetaMap.get(entityClass);
        Validator.validateTrue(meta.isClusteredEntity(),
                "Cannot perform slice query on entity type '%s' because it is " + "not a clustered entity",
                meta.getClassName());
        return new SliceQueryBuilder<>(sliceQueryExecutor, entityClass, meta);
    }

    /**
     * Return a CQL native query builder
     *
     * <br/>
     * <br/>
     *
     *  <h3>Native query without bound values</h3>
     *  <pre class="code"><code class="java">
     *      RegularStatement nativeQuery = select("name",age_in_years").from("UserEntity").where(in("id",Arrays.asList(10,11))).limit(20);
     *      List&lt;TypedMap&gt; actual = manager.nativeQuery(nativeQuery).get();
     *  </code></pre>
     *
     *  <br/>
     *  <br/>
     *
     *  <h3>Native query with bound values</h3>
     *  <pre class="code"><code class="java">
     *      RegularStatement nativeQuery = select("name",age_in_years").from("UserEntity").where(in("id",bindMarker())).limit(bindMarker());
     *      List&lt;TypedMap&gt; actual = manager.nativeQuery(nativeQuery,Arrays.asList(10,11),20).get();
     *  </code></pre>
     *
     * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#native-query" target="_blank">Native query API</a>
     *
     * @param regularStatement
     *            native CQL3 regularStatement, including limit, ttl and consistency
     *            options
     *
     * @param boundValues
     *            values to be bind to the parameterized query, if any
     *
     * @return NativeQuery
     */
    public NativeQuery nativeQuery(RegularStatement regularStatement, Object... boundValues) {
        return this.nativeQuery(regularStatement, noOptions(), boundValues);
    }

    /**
     * Return a CQL native query builder
     *
     * <br/>
     * <br/>
     *
     *  <h3>Native query without bound values</h3>
     *  <pre class="code"><code class="java">
     *      RegularStatement nativeQuery = select("name",age_in_years").from("UserEntity").where(in("id",Arrays.asList(10,11))).limit(20);
     *      List&lt;TypedMap&gt; actual = manager.nativeQuery(nativeQuery).get();
     *  </code></pre>
     *
     *  <br/>
     *  <br/>
     *
     *  <h3>Native query with bound values</h3>
     *  <pre class="code"><code class="java">
     *      RegularStatement nativeQuery = select("name",age_in_years").from("UserEntity").where(in("id",bindMarker())).limit(bindMarker());
     *      List&lt;TypedMap&gt; actual = manager.nativeQuery(nativeQuery,Arrays.asList(10,11),20).get();
     *  </code></pre>
     *
     * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#native-query" target="_blank">Native query API</a>
     *
     * @param queryString
     *            native CQL query string, including limit, ttl and consistency
     *            options
     *
     * @param options
     *            options
     *
     * @param boundValues
     *            values to be bind to the parameterized query, if any
     *
     * @return NativeQuery
     */
    public NativeQuery nativeQuery(RegularStatement regularStatement, Options options, Object... boundValues) {
        log.debug("Execute native query {}", regularStatement);
        Validator.validateNotNull(regularStatement, "The regularStatement for native query should not be null");
        return new NativeQuery(daoContext, regularStatement, options, boundValues);
    }

    /**
     * Return a CQL typed query builder
     *
     * All found entities will be in <strong>managed</strong> state
     *
     * <br/>
     * <br/>
     *
     *  <h3>Typed query without bound values</h3>
     *  <pre class="code"><code class="java">
     *      RegularStatement nativeQuery = select().from("MyEntity").where().limit(3);
     *      List&lt;MyEntity> actual = manager.typedQuery(MyEntity.class, nativeQuery).get();
     *  </code></pre>
     *
     *  <br/>
     *  <br/>
     *
     *  <h3>Typed query with bound values</h3>
     *  <pre class="code"><code class="java">
     *      RegularStatement statement = select().from("MyEntity").limit(bindMarker());
     *      List&lt;MyEntity&gt; actual = manager.typedQuery(MyEntity.class, statement,3).get();
     *  </code></pre>
     *
     * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#typed-query" target="_blank">Typed query API</a>
     *
     * @param entityClass
     *            type of entity to be returned
     *
     * @param regularStatement
     *            native CQL3 regularStatement, including limit, ttl and consistency
     *            options
     *
     * @param boundValues
     *            values to be bind to the parameterized query, if any
     *
     * @return TypedQuery<T>
     */
    public <T> TypedQuery<T> typedQuery(Class<T> entityClass, RegularStatement regularStatement, Object... boundValues) {
        return typedQueryInternal(entityClass, regularStatement, boundValues);
    }

    private <T> TypedQuery<T> typedQueryInternal(Class<T> entityClass, RegularStatement regularStatement, Object... boundValues) {
        log.debug("Execute typed query for entity class {}", entityClass);
        Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
        Validator.validateNotNull(regularStatement, "The regularStatement for typed query should not be null");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass),
                "Cannot perform typed query because the entityClass '%s' is not managed by Achilles",
                entityClass.getCanonicalName());

        EntityMeta meta = entityMetaMap.get(entityClass);
        typedQueryValidator.validateTypedQuery(entityClass, regularStatement, meta);
        return new TypedQuery<>(entityClass, daoContext, regularStatement, meta, contextFactory, MANAGED, boundValues);
    }

    /**
     * Return a CQL typed query builder
     *
     * All found entities will be in 'managed' state
     *
     * @param entityClass
     *            type of entity to be returned
     *
     * @param indexCondition
     *            index condition
     *
     * @return TypedQuery<T>
     */
    public <T> TypedQuery<T> indexedQuery(Class<T> entityClass, IndexCondition indexCondition) {
        log.debug("Execute indexed query for entity class {}", entityClass);

        EntityMeta entityMeta = entityMetaMap.get(entityClass);

        Validator.validateFalse(entityMeta.isClusteredEntity(), "Index query is not supported for clustered entity. Please use typed query/native query");
        Validator.validateNotNull(indexCondition, "Index condition should not be null");

        entityMeta.encodeIndexConditionValue(indexCondition);

        String indexColumnName = indexCondition.getColumnName();
        final Select.Where statement = select().from(entityMeta.getTableName()).where(eq(indexColumnName, bindMarker(indexColumnName)));
        return typedQueryInternal(entityClass, statement, indexCondition.getColumnValue());
    }

    /**
     * Return a CQL typed query builder
     *
     * All found entities will be returned as raw entities and not 'managed' by
     * Achilles
     *
     * <br/>
     * <br/>
     *
     *  <h3>Raw typed query without bound values</h3>
     *  <pre class="code"><code class="java">
     *      RegularStatement nativeQuery = select().from("MyEntity").where().limit(3);
     *      List&lt;MyEntity> actual = manager.rawTypedQuery(MyEntity.class, nativeQuery).get();
     *  </code></pre>
     *
     *  <br/>
     *  <br/>
     *
     *  <h3>Raw typed query with bound values</h3>
     *  <pre class="code"><code class="java">
     *      RegularStatement nativeQuery = select().from("MyEntity").where().limit(bindMarker());
     *      List&lt;MyEntity&gt; actual = manager.rawTypedQuery(MyEntity.class, nativeQuery,3).get();
     *  </code></pre>
     *
     * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#typed-query" target="_blank">Typed query API</a>
     *
     * @param entityClass
     *            type of entity to be returned
     *
     * @param regularStatement
     *            native CQL3 regularStatement, including limit, ttl and consistency
     *            options
     *
     * @param boundValues
     *            values to be bind to the parameterized query, if any
     *
     * @return TypedQuery<T>
     */
    public <T> TypedQuery<T> rawTypedQuery(Class<T> entityClass, RegularStatement regularStatement, Object... boundValues) {
        log.debug("Execute raw typed query for entity class {}", entityClass);
        Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
        Validator.validateNotNull(regularStatement, "The regularStatement for typed query should not be null");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass),
                "Cannot perform typed query because the entityClass '%s' is not managed by Achilles",
                entityClass.getCanonicalName());

        EntityMeta meta = entityMetaMap.get(entityClass);
        typedQueryValidator.validateRawTypedQuery(entityClass, regularStatement, meta);
        return new TypedQuery<>(entityClass, daoContext, regularStatement, meta, contextFactory, NOT_MANAGED, boundValues);
    }

    /**
     * Serialize the entity in JSON using a registered Object Mapper or default Achilles Object Mapper
     * @param entity
     * @return serialized entity in JSON
     * @throws IOException
     */
    public String serializeToJSON(Object entity) throws IOException {
        Validator.validateNotNull(entity, "Cannot serialize to JSON null entity");
        final ObjectMapper objectMapper = configContext.getMapperFor(entity.getClass());
        return objectMapper.writeValueAsString(entity);
    }

    /**
     * Deserialize the given JSON into entity using a registered Object Mapper or default Achilles Object Mapper
     * @param type
     * @param serialized
     * @param <T>
     * @return deserialized entity from JSON
     * @throws IOException
     */
    public <T> T deserializeFromJSON(Class<T> type, String serialized) throws IOException {
        Validator.validateNotNull(type, "Cannot deserialize from JSON if target type is null");
        final ObjectMapper objectMapper = configContext.getMapperFor(type);
        return objectMapper.readValue(serialized, type);
    }

    protected PersistenceManagerOperations initPersistenceContext(Class<?> entityClass, Object primaryKey, Options options) {
        return contextFactory.newContext(entityClass, primaryKey, options).getPersistenceManagerFacade();
    }

    protected PersistenceManagerOperations initPersistenceContext(Object entity, Options options) {
        return contextFactory.newContext(entity, options).getPersistenceManagerFacade();
    }

    /**
     * Return Session object from Java Driver
     * @return Session
     */
    public Session getNativeSession() {
        return daoContext.getSession();
    }

    /**
     * Create a new state-full Batch <br/>
     * <br/>
     * <p/>
     * <strong>WARNING : This Batch is state-full and not
     * thread-safe. In case of exception, you MUST not re-use it but create
     * another one</strong>
     *
     * @return a new state-full PersistenceManager
     */
    public Batch createBatch() {
        log.debug("Spawn new BatchingPersistenceManager");
        return new Batch(entityMetaMap, contextFactory, daoContext, configContext, false);
    }


    /**
     * Create a new state-full <strong>ordered</strong> Batch <br/>
     * <br/>
     * <p>
     * This Batch respect insertion order by generating increasing timestamp with micro second resolution.
     * If you use ordered Batch in multiple clients, do not forget to synchronize the clock between those clients
     * to avoid statements interleaving
     * </p>
     * <strong>WARNING : This Batch is state-full and not
     * thread-safe. In case of exception, you MUST not re-use it but create
     * another one</strong>
     *
     * @return a new state-full PersistenceManager
     */
    public Batch createOrderedBatch() {
        log.debug("Spawn new BatchingPersistenceManager");
        return new Batch(entityMetaMap, contextFactory, daoContext, configContext, true);
    }

    protected Map<Class<?>, EntityMeta> getEntityMetaMap() {
        return entityMetaMap;
    }

    protected ConfigurationContext getConfigContext() {
        return configContext;
    }

    protected void setEntityMetaMap(Map<Class<?>, EntityMeta> entityMetaMap) {
        this.entityMetaMap = entityMetaMap;
    }

    protected void setConfigContext(ConfigurationContext configContext) {
        this.configContext = configContext;
    }
}
