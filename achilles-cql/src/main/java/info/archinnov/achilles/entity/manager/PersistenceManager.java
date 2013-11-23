/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.context.PersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.query.cql.CQLNativeQueryBuilder;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.query.typed.TypedQueryBuilder;
import info.archinnov.achilles.query.typed.TypedQueryValidator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

public class PersistenceManager {
	private static final Logger log = LoggerFactory.getLogger(PersistenceManager.class);

	protected Map<Class<?>, EntityMeta> entityMetaMap;
	protected ConfigurationContext configContext;
	protected PersistenceContextFactory contextFactory;

	protected EntityProxifier proxifier = new EntityProxifier();
	private EntityValidator entityValidator = new EntityValidator();
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
	 * @param entity
	 *            Entity to be persisted
	 */
	public void persist(Object entity) {
		log.debug("Persisting entity '{}'", entity);

		persist(entity, OptionsBuilder.noOptions());
	}

	/**
	 * Persist an entity with the given options.
	 * 
	 * @param entity
	 *            Entity to be persisted
	 * @param options
	 *            options for consistency level, ttl and timestamp
	 */
	public void persist(final Object entity, Options options) {
		if (log.isDebugEnabled())
			log.debug("Persisting entity '{}' with options {} ", entity, options);

		entityValidator.validateEntity(entity, entityMetaMap);

		if (options.getTtl().isPresent()) {
			entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
		}
		if (proxifier.isProxy(entity)) {
			throw new IllegalStateException(
					"Then entity is already in 'managed' state. Please use the merge() method instead of persist()");
		}

		PersistenceContext context = initPersistenceContext(entity, options);
		context.persist();
	}

	/**
	 * Merge an entity.
	 * 
	 * Calling merge on a transient entity will persist it and returns a managed
	 * 
	 * instance.
	 * 
	 * <strong>Unlike the JPA specs, Achilles returns the same entity passed
	 * 
	 * in parameter if the latter is in managed state. It was designed on
	 * purpose
	 * 
	 * so you do not loose the reference of the passed entity. For transient
	 * 
	 * entity, the return value is a new proxy object
	 * 
	 * </strong>
	 * 
	 * @param entity
	 *            Entity to be merged
	 * @return Merged entity or a new proxified entity
	 */
	public <T> T merge(T entity) {
		if (log.isDebugEnabled())
			log.debug("Merging entity '{}'", proxifier.unwrap(entity));

		return merge(entity, OptionsBuilder.noOptions());
	}

	/**
	 * Merge an entity with the given options
	 * 
	 * Calling merge on a transient entity will persist it and returns a managed
	 * instance.
	 * 
	 * <strong>Unlike the JPA specs, Achilles returns the same entity passed
	 * 
	 * in parameter if the latter is in managed state. It was designed on
	 * purpose
	 * 
	 * so you do not loose the reference of the passed entity. For transient
	 * 
	 * entity, the return value is a new proxy object
	 * 
	 * </strong>
	 * 
	 * @param entity
	 *            Entity to be merged
	 * @param options
	 *            options for consistency level, ttl and timestamp
	 * @return Merged entity or a new proxified entity
	 */
	public <T> T merge(final T entity, Options options) {
		if (log.isDebugEnabled()) {
			log.debug("Merging entity '{}' with options {} ", proxifier.unwrap(entity), options);
		}
		entityValidator.validateEntity(entity, entityMetaMap);
		if (options.getTtl().isPresent()) {
			entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
		}
		PersistenceContext context = initPersistenceContext(entity, options);
		return context.<T> merge(entity);

	}

	/**
	 * Remove an entity.
	 * 
	 * @param entity
	 *            Entity to be removed
	 */
	public void remove(Object entity) {
		if (log.isDebugEnabled())
			log.debug("Removing entity '{}'", proxifier.unwrap(entity));

		remove(entity, null);
	}

	/**
	 * Remove an entity by its id.
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
		PersistenceContext context = initPersistenceContext(entityClass, primaryKey, OptionsBuilder.noOptions());
		entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
		context.remove();
	}

	/**
	 * Remove an entity with the given Consistency Level for write.
	 * 
	 * @param entity
	 *            Entity to be removed
	 * @param writeLevel
	 *            Consistency Level for write
	 */
	public void remove(final Object entity, ConsistencyLevel writeLevel) {
		if (log.isDebugEnabled())
			log.debug("Removing entity '{}' with write consistency level {}", proxifier.unwrap(entity), writeLevel);

        Object realObject = proxifier.getRealObject(entity);
        entityValidator.validateEntity(realObject, entityMetaMap);
		PersistenceContext context = initPersistenceContext(realObject, OptionsBuilder.withConsistency(writeLevel));
		context.remove();
	}

	/**
	 * Remove an entity by its id with the given Consistency Level for write.
	 * 
	 * @param entityClass
	 *            Entity class
	 * 
	 * @param primaryKey
	 *            Primary key
	 */
	public void removeById(Class<?> entityClass, Object primaryKey, ConsistencyLevel writeLevel) {
		Validator.validateNotNull(entityClass, "The entity class should not be null for removal by id");
		Validator.validateNotNull(primaryKey, "The primary key should not be null for removal by id");
		if (log.isDebugEnabled())
			log.debug("Removing entity of type '{}' by its id '{}'", entityClass, primaryKey);

		PersistenceContext context = initPersistenceContext(entityClass, primaryKey,
                                                               OptionsBuilder.withConsistency(writeLevel));
		entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
		context.remove();
	}

	/**
	 * Find an entity.
	 * 
	 * @param entityClass
	 *            Entity type
	 * @param primaryKey
	 *            Primary key (Cassandra row key) of the entity to load
	 */
	public <T> T find(Class<T> entityClass, Object primaryKey) {
		log.debug("Find entity class '{}' with primary key {}", entityClass, primaryKey);
		return find(entityClass, primaryKey, null);
	}

	/**
	 * Find an entity with the given Consistency Level for read
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
		PersistenceContext context = initPersistenceContext(entityClass, primaryKey,
                                                               OptionsBuilder.withConsistency(readLevel));
		entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
		return context.<T> find(entityClass);
	}

	/**
	 * Find an entity. Works exactly as find(Class<T> entityClass, Object
	 * primaryKey) except that the database will not be hit. This method never
	 * returns null
	 * 
	 * @param entityClass
	 *            Entity type
	 * @param primaryKey
	 *            Primary key (Cassandra row key) of the entity to initialize
	 */
	public <T> T getReference(Class<T> entityClass, Object primaryKey) {
		if (log.isDebugEnabled())
			log.debug("Get reference for entity class '{}' with primary key {}", entityClass, primaryKey);

		return getReference(entityClass, primaryKey, null);
	}

	/**
	 * Find an entity with the given Consistency Level for read. Works exactly
	 * as find(Class<T> entityClass, Object primaryKey) except that the database
	 * will not be hit. This method never returns null
	 * 
	 * @param entityClass
	 *            Entity type
	 * @param primaryKey
	 *            Primary key (Cassandra row key) of the entity to initialize
	 * @param readLevel
	 *            Consistency Level for read
	 */
	public <T> T getReference(final Class<T> entityClass, final Object primaryKey, ConsistencyLevel readLevel) {
		if (log.isDebugEnabled())
			log.debug("Get reference for entity class '{}' with primary key {} and read consistency level {}",
					entityClass, primaryKey, readLevel);

		Validator.validateNotNull(entityClass, "Entity class should not be null for get reference");
		Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null for get reference");
		Validator.validateTrue(entityMetaMap.containsKey(entityClass),
				"The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());

		PersistenceContext context = initPersistenceContext(entityClass, primaryKey,
                                                               OptionsBuilder.withConsistency(readLevel));
		entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
		return context.<T> getReference(entityClass);
	}

	/**
	 * Refresh an entity.
	 * 
	 * @param entity
	 *            Entity to be refreshed
	 */
	public void refresh(Object entity) throws AchillesStaleObjectStateException {
		if (log.isDebugEnabled())
			log.debug("Refreshing entity '{}'", proxifier.unwrap(entity));

		refresh(entity, null);
	}

	/**
	 * Refresh an entity with the given Consistency Level for read.
	 * 
	 * @param entity
	 *            Entity to be refreshed
	 * @param readLevel
	 *            Consistency Level for read
	 */
	public void refresh(final Object entity, ConsistencyLevel readLevel) throws AchillesStaleObjectStateException {
		if (log.isDebugEnabled())
			log.debug("Refreshing entity '{}' with read consistency level {}", proxifier.unwrap(entity), readLevel);

		entityValidator.validateEntity(entity, entityMetaMap);
		proxifier.ensureProxy(entity);
		PersistenceContext context = initPersistenceContext(entity, OptionsBuilder.withConsistency(readLevel));
		context.refresh();
	}

	/**
	 * Initialize all lazy fields of a 'managed' entity, except WideMap/Counter
	 * fields.
	 * 
	 * Raise an <strong>IllegalStateException</strong> if the entity is not
	 * 'managed'
	 * 
	 */
	public <T> T initialize(final T entity) {
		log.debug("Force lazy fields initialization for entity {}", entity);
		proxifier.ensureProxy(entity);
		PersistenceContext context = initPersistenceContext(entity, OptionsBuilder.noOptions());
		return context.initialize(entity);
	}

	/**
	 * Initialize all lazy fields of a set of 'managed' entities, except
	 * WideMap/Counter fields.
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
	 * Initialize all lazy fields of a list of 'managed' entities, except
	 * WideMap/Counter fields.
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
	 * Shorthand for manager.unwrap(manager.initialize(T entity))
	 * 
	 */
	public <T> T initAndUnwrap(T entity) {
		return unwrap(initialize(entity));
	}

	/**
	 * Shorthand for manager.unwrap(manager.initialize(Set<T> entities))
	 * 
	 */
	public <T> Set<T> initAndUnwrap(Set<T> entities) {
		return unwrap(initialize(entities));
	}

	/**
	 * Shorthand for manager.unwrap(manager.initialize(List<T> entities))
	 * 
	 */
	public <T> List<T> initAndUnwrap(List<T> entities) {
		return unwrap(initialize(entities));
	}

	/**
	 * Unwrap a 'managed' entity to prepare it for serialization
	 * 
	 * If the argument is not a proxy objet, return itself <br/>
	 * Else, return the target object behind the proxy
	 * 
	 * @param proxy
	 * @return real object
	 */
	public <T> T unwrap(T proxy) {
		log.debug("Unproxying entity {}", proxy);

		T realObject = proxifier.unwrap(proxy);

		return realObject;
	}

	/**
	 * Unwrap a list of 'managed' entities to prepare them for serialization
	 * 
	 * See {@link #unwrap}
	 * 
	 * @param proxies
	 *            list of proxified entity
	 * @return real object list
	 */
	public <T> List<T> unwrap(List<T> proxies) {
		log.debug("Unproxying list of entities {}", proxies);

		return proxifier.unwrap(proxies);
	}

	/**
	 * Unwrap a set of 'managed' entities to prepare them for serialization
	 * 
	 * See {@link #unwrap}
	 * 
	 * @param proxies
	 *            set of proxified entities
	 * @return real object set
	 */
	public <T> Set<T> unwrap(Set<T> proxies) {
		log.debug("Unproxying set of entities {}", proxies);

		return proxifier.unwrap(proxies);
	}

	public <T> SliceQueryBuilder<T> sliceQuery(Class<T> entityClass) {
		EntityMeta meta = entityMetaMap.get(entityClass);
		Validator.validateTrue(meta.isClusteredEntity(),
				"Cannot perform slice query on entity type '%s' because it is " + "not a clustered entity",
				meta.getClassName());
		return new SliceQueryBuilder<T>(sliceQueryExecutor, entityClass, meta);
	}

	/**
	 * Return a CQL native query builder
	 * 
	 * @param queryString
	 *            native CQL query string, including limit, ttl and consistency
	 *            options
	 * 
	 * @return CQLNativeQueryBuilder
	 */
	public CQLNativeQueryBuilder nativeQuery(String queryString) {
		Validator.validateNotBlank(queryString, "The query string for native query should not be blank");
		return new CQLNativeQueryBuilder(daoContext, queryString);
	}

	/**
	 * Return a CQL typed query builder
	 * 
	 * All found entities will be in 'managed' state
	 * 
	 * @param entityClass
	 *            type of entity to be returned
	 * 
	 * @param queryString
	 *            native CQL query string, including limit, ttl and consistency
	 *            options
	 * 
	 * @return TypedQueryBuilder<T>
	 */
	public <T> TypedQueryBuilder<T> typedQuery(Class<T> entityClass, String queryString) {
		return typedQuery(entityClass, queryString, true);
	}

	private <T> TypedQueryBuilder<T> typedQuery(Class<T> entityClass, String queryString, boolean normalizeQuery) {
		Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
		Validator.validateNotBlank(queryString, "The query string for typed query should not be blank");
		Validator.validateTrue(entityMetaMap.containsKey(entityClass),
				"Cannot perform typed query because the entityClass '%s' is not managed by Achilles",
				entityClass.getCanonicalName());

		EntityMeta meta = entityMetaMap.get(entityClass);
		typedQueryValidator.validateTypedQuery(entityClass, queryString, meta);
		return new TypedQueryBuilder<T>(entityClass, daoContext, queryString, meta, contextFactory, true,
				normalizeQuery);
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
	 * @return TypedQueryBuilder<T>
	 */
	public <T> TypedQueryBuilder<T> indexedQuery(Class<T> entityClass, IndexCondition indexCondition) {
		EntityMeta entityMeta = entityMetaMap.get(entityClass);

		Validator.validateFalse(entityMeta.isClusteredEntity(),
				"Index query is not supported for clustered entity. Please use typed query/native query");
		Validator.validateNotNull(indexCondition, "Index condition should not be null");
		Validator.validateNotBlank(indexCondition.getColumnName(),
				"Column name for index condition '%s' should be provided", indexCondition);
		Validator.validateNotNull(indexCondition.getColumnValue(),
				"Column value for index condition '%s' should be provided", indexCondition);
		Validator.validateNotNull(indexCondition.getIndexRelation(),
				"Index relation for index condition '%s' should be provided", indexCondition);

        final Select.Where query = QueryBuilder.select().from(entityMeta.getTableName())
                                               .where(QueryBuilder.eq(indexCondition.getColumnName(), indexCondition
                                                       .getColumnValue()));
		return typedQuery(entityClass, query.getQueryString(), false);
	}

	/**
	 * Return a CQL typed query builder
	 * 
	 * All found entities will be returned as raw entities and not 'managed' by
	 * Achilles
	 * 
	 * @param entityClass
	 *            type of entity to be returned
	 * 
	 * @param queryString
	 *            native CQL query string, including limit, ttl and consistency
	 *            options
	 * 
	 * @return TypedQueryBuilder<T>
	 */
	public <T> TypedQueryBuilder<T> rawTypedQuery(Class<T> entityClass, String queryString) {
		Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
		Validator.validateNotBlank(queryString, "The query string for typed query should not be blank");
		Validator.validateTrue(entityMetaMap.containsKey(entityClass),
				"Cannot perform typed query because the entityClass '%s' is not managed by Achilles",
				entityClass.getCanonicalName());

		EntityMeta meta = entityMetaMap.get(entityClass);
		typedQueryValidator.validateRawTypedQuery(entityClass, queryString, meta);
		return new TypedQueryBuilder<T>(entityClass, daoContext, queryString, meta, contextFactory, false, true);
	}

    protected PersistenceContext initPersistenceContext(Class<?> entityClass, Object primaryKey, Options options) {
        return contextFactory.newContext(entityClass, primaryKey, options);
    }

    protected PersistenceContext initPersistenceContext(Object entity, Options options) {
        return contextFactory.newContext(entity, options);
    }

	public Session getNativeSession() {
		return daoContext.getSession();
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
