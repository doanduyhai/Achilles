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

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public abstract class EntityManager<CONTEXT extends PersistenceContext> {
	protected static final Optional<Integer> NO_TTL = Optional
			.<Integer> absent();
	protected static final Optional<ConsistencyLevel> NO_CONSISTENCY_LEVEL = Optional
			.<ConsistencyLevel> absent();

	private static final Logger log = LoggerFactory
			.getLogger(EntityManager.class);

	protected Map<Class<?>, EntityMeta> entityMetaMap;
	protected AchillesConsistencyLevelPolicy consistencyPolicy;
	protected ConfigurationContext configContext;

	protected EntityProxifier<CONTEXT> proxifier;
	protected EntityValidator<CONTEXT> entityValidator;
	protected EntityInitializer initializer = new EntityInitializer();

	EntityManager(Map<Class<?>, EntityMeta> entityMetaMap, //
			ConfigurationContext configContext) {
		this.entityMetaMap = entityMetaMap;
		this.configContext = configContext;
		this.consistencyPolicy = configContext.getConsistencyPolicy();
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
	 * @param Options
	 *            options for consistency level, ttl and timestamp
	 */
	public void persist(final Object entity, Options options) {
		if (log.isDebugEnabled())
			log.debug("Persisting entity '{}' with options {} ", entity,
					options);

		entityValidator.validateEntity(entity, entityMetaMap);

		if (options.getTtl().isPresent()) {
			entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
		}
		if (proxifier.isProxy(entity)) {
			throw new IllegalStateException(
					"Then entity is already in 'managed' state. Please use the merge() method instead of persist()");
		}

		CONTEXT context = initPersistenceContext(entity, options);
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
	 * @param Options
	 *            options for consistency level, ttl and timestamp
	 * @return Merged entity or a new proxified entity
	 */
	public <T> T merge(final T entity, Options options) {
		if (log.isDebugEnabled()) {
			log.debug("Merging entity '{}' with options {} ",
					proxifier.unwrap(entity), options);
		}
		entityValidator.validateEntity(entity, entityMetaMap);
		if (options.getTtl().isPresent()) {
			entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
		}
		CONTEXT context = initPersistenceContext(entity, options);
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

		Validator.validateNotNull(entityClass,
				"The entity class should not be null for removal by id");
		Validator.validateNotNull(primaryKey,
				"The primary key should not be null for removal by id");
		if (log.isDebugEnabled()) {
			log.debug("Removing entity of type '{}' by its id '{}'",
					entityClass, primaryKey);
		}
		CONTEXT context = initPersistenceContext(entityClass, primaryKey,
				OptionsBuilder.noOptions());
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
			log.debug("Removing entity '{}' with write consistency level {}",
					proxifier.unwrap(entity), writeLevel);

		entityValidator.validateEntity(entity, entityMetaMap);
		proxifier.ensureProxy(entity);
		CONTEXT context = initPersistenceContext(entity,
				OptionsBuilder.withConsistency(writeLevel));
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
	public void removeById(Class<?> entityClass, Object primaryKey,
			ConsistencyLevel writeLevel) {
		Validator.validateNotNull(entityClass,
				"The entity class should not be null for removal by id");
		Validator.validateNotNull(primaryKey,
				"The primary key should not be null for removal by id");
		if (log.isDebugEnabled())
			log.debug("Removing entity of type '{}' by its id '{}'",
					entityClass, primaryKey);

		CONTEXT context = initPersistenceContext(entityClass, primaryKey,
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
	 * @param entity
	 *            Found entity or null if no entity is found
	 */
	public <T> T find(Class<T> entityClass, Object primaryKey) {
		log.debug("Find entity class '{}' with primary key {}", entityClass,
				primaryKey);
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
	 * @param entity
	 *            Found entity or null if no entity is found
	 */
	public <T> T find(final Class<T> entityClass, final Object primaryKey,
			ConsistencyLevel readLevel) {
		log.debug(
				"Find entity class '{}' with primary key {} and read consistency level {}",
				entityClass, primaryKey, readLevel);
		Validator.validateNotNull(entityClass,
				"Entity class should not be null for find by id");
		Validator.validateNotNull(primaryKey,
				"Entity primaryKey should not be null for find by id");
		CONTEXT context = initPersistenceContext(entityClass, primaryKey,
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
	 * @param entity
	 *            Proxified empty entity
	 */
	public <T> T getReference(Class<T> entityClass, Object primaryKey) {
		if (log.isDebugEnabled())
			log.debug(
					"Get reference for entity class '{}' with primary key {}",
					entityClass, primaryKey);

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
	 * @param entity
	 *            Proxified empty entity
	 */
	public <T> T getReference(final Class<T> entityClass,
			final Object primaryKey, ConsistencyLevel readLevel) {
		if (log.isDebugEnabled())
			log.debug(
					"Get reference for entity class '{}' with primary key {} and read consistency level {}",
					entityClass, primaryKey, readLevel);

		Validator.validateNotNull(entityClass,
				"Entity class should not be null for get reference");
		Validator.validateNotNull(primaryKey,
				"Entity primaryKey should not be null for get reference");
		CONTEXT context = initPersistenceContext(entityClass, primaryKey,
				OptionsBuilder.withConsistency(readLevel));
		entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
		return context.<T> getReference(entityClass);
	}

	/**
	 * Refresh an entity.
	 * 
	 * will be also refreshed from Cassandra.
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
	public void refresh(final Object entity, ConsistencyLevel readLevel)
			throws AchillesStaleObjectStateException {
		if (log.isDebugEnabled())
			log.debug("Refreshing entity '{}' with read consistency level {}",
					proxifier.unwrap(entity), readLevel);

		entityValidator.validateEntity(entity, entityMetaMap);
		proxifier.ensureProxy(entity);
		CONTEXT context = initPersistenceContext(entity,
				OptionsBuilder.withConsistency(readLevel));
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
		CONTEXT context = initPersistenceContext(entity,
				OptionsBuilder.noOptions());
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
		log.debug("Force lazy fields initialization for entity set {}",
				entities);
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
		log.debug("Force lazy fields initialization for entity set {}",
				entities);
		for (T entity : entities) {
			initialize(entity);
		}
		return entities;
	}

	/**
	 * Shorthand for em.unwrap(em.initialize(T entity))
	 * 
	 */
	public <T> T initAndUnwrap(T entity) {
		return unwrap(initialize(entity));
	}

	/**
	 * Shorthand for em.unwrap(em.initialize(Set<T> entities))
	 * 
	 */
	public <T> Set<T> initAndUnwrap(Set<T> entities) {
		return unwrap(initialize(entities));
	}

	/**
	 * Shorthand for em.unwrap(em.initialize(List<T> entities))
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
	 * @param proxy
	 *            list
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
	 * @param proxy
	 *            set
	 * @return real object set
	 */
	public <T> Set<T> unwrap(Set<T> proxies) {
		log.debug("Unproxying set of entities {}", proxies);

		return proxifier.unwrap(proxies);
	}

	/**
	 * Create a new slice query builder for entity of type T<br/>
	 * <br/>
	 * 
	 * @param entityClass
	 *            Entity class
	 * @return SliceQueryBuilder<T>
	 */
	public abstract <T> SliceQueryBuilder<CONTEXT, T> sliceQuery(
			Class<T> entityClass);

	protected abstract CONTEXT initPersistenceContext(Object entity,
			Options options);

	protected abstract CONTEXT initPersistenceContext(Class<?> entityClass,
			Object primaryKey, Options options);

	protected Map<Class<?>, EntityMeta> getEntityMetaMap() {
		return entityMetaMap;
	}

	protected AchillesConsistencyLevelPolicy getConsistencyPolicy() {
		return consistencyPolicy;
	}

	protected ConfigurationContext getConfigContext() {
		return configContext;
	}

	protected void setProxifier(EntityProxifier<CONTEXT> proxifier) {
		this.proxifier = proxifier;
	}

	protected void setEntityValidator(EntityValidator<CONTEXT> entityValidator) {
		this.entityValidator = entityValidator;
	}

	protected void setInitializer(EntityInitializer initializer) {
		this.initializer = initializer;
	}

	protected void setEntityMetaMap(Map<Class<?>, EntityMeta> entityMetaMap) {
		this.entityMetaMap = entityMetaMap;
	}

	protected void setConsistencyPolicy(
			AchillesConsistencyLevelPolicy consistencyPolicy) {
		this.consistencyPolicy = consistencyPolicy;
	}

	protected void setConfigContext(ConfigurationContext configContext) {
		this.configContext = configContext;
	}

}
