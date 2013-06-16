package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * AchillesEntityManager
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AchillesEntityManager<CONTEXT extends AchillesPersistenceContext>
{
	private static final Logger log = LoggerFactory.getLogger(AchillesEntityManager.class);

	protected final AchillesEntityManagerFactory entityManagerFactory;
	protected Map<Class<?>, EntityMeta> entityMetaMap;
	protected AchillesConsistencyLevelPolicy consistencyPolicy;
	protected ConfigurationContext configContext;

	protected EntityProxifier<CONTEXT> proxifier;
	protected EntityValidator<CONTEXT> entityValidator;
	protected EntityInitializer initializer = new EntityInitializer();

	AchillesEntityManager(AchillesEntityManagerFactory entityManagerFactory,
			Map<Class<?>, EntityMeta> entityMetaMap, //
			ConfigurationContext configContext)
	{
		this.entityManagerFactory = entityManagerFactory;
		this.entityMetaMap = entityMetaMap;
		this.configContext = configContext;
		this.consistencyPolicy = configContext.getConsistencyPolicy();
	}

	/**
	 * Persist an entity. All join entities with CascadeType.PERSIST or CascadeType.ALL
	 * 
	 * will be also persisted, overriding their current state in Cassandra
	 * 
	 * @param entity
	 *            Entity to be persisted
	 */
	public void persist(Object entity)
	{
		log.debug("Persisting entity '{}'", entity);

		persist(entity, Optional.<ConsistencyLevel> absent(), Optional.<Integer> absent());
	}

	/**
	 * Persist an entity with the given Consistency Level for write. All join entities with CascadeType.PERSIST or CascadeType.ALL
	 * 
	 * will be also persisted with the same Consistency Level, overriding their current state in Cassandra
	 * 
	 * @param entity
	 *            Entity to be persisted
	 * @param writeLevel
	 *            Consistency Level for write
	 */
	public void persist(final Object entity, ConsistencyLevel writeLevel)
	{
		log.debug("Persisting entity '{}' with write consistency level {}", entity, writeLevel);

		persist(entity, Optional.fromNullable(writeLevel), Optional.<Integer> absent());
	}

	/**
	 * Persist an entity with the given time-to-live. All join entities with CascadeType.PERSIST or CascadeType.ALL
	 * 
	 * will be also persisted <strong>but the time-to-live will not be cascaded</strong>
	 * 
	 * @param entity
	 *            Entity to be persisted
	 * @param ttl
	 *            Time to live
	 */
	public void persist(final Object entity, int ttl)
	{
		log.debug("Persisting entity '{}' with ttl {}", entity, ttl);

		persist(entity, Optional.<ConsistencyLevel> absent(), Optional.fromNullable(ttl));
	}

	/**
	 * Persist an entity with the given time-to-live and Consistency Level. All join entities
	 * 
	 * with CascadeType.PERSIST or CascadeType.ALL will be also persisted with the same
	 * 
	 * Consistency Level <strong>but the time-to-live will not be cascaded</strong>
	 * 
	 * @param entity
	 *            Entity to be persisted
	 * @param ttl
	 *            Time to live
	 * @param writeLevel
	 *            Consistency Level for write
	 */
	public void persist(final Object entity, int ttl, ConsistencyLevel writeLevel)
	{
		log.debug("Persisting entity '{}' with ttl {} and consistency level {}", entity, ttl,
				writeLevel);

		persist(entity, Optional.fromNullable(writeLevel), Optional.fromNullable(ttl));
	}

	void persist(final Object entity, Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
	{
		entityValidator.validateEntity(entity, entityMetaMap);
		entityValidator.validateNotWideRow(entity, entityMetaMap);

		if (proxifier.isProxy(entity))
		{
			throw new IllegalStateException(
					"Then entity is already in 'managed' state. Please use the merge() method instead of persist()");
		}

		CONTEXT context = initPersistenceContext(entity, Optional.<ConsistencyLevel> absent(),
				writeLevelO, ttlO);
		context.persist();
	}

	/**
	 * Merge an entity. All join entities with CascadeType.MERGE or CascadeType.ALL
	 * 
	 * will be also merged, updating their current state in Cassandra.
	 * 
	 * Calling merge on a transient entity will persist it and returns a managed
	 * 
	 * instance.
	 * 
	 * <strong>Unlike the JPA specs, Achilles returns the same entity passed
	 * 
	 * in parameter if the latter is in managed state. It was designed on purpose
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
	public <T> T merge(T entity)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Merging entity '{}'", proxifier.unproxy(entity));
		}
		return merge(entity, Optional.<ConsistencyLevel> absent(), Optional.<Integer> absent());
	}

	/**
	 * Merge an entity with the given Consistency Level for write. All join entities with CascadeType.MERGE or CascadeType.ALL
	 * 
	 * will be also merged, updating their current state in Cassandra.
	 * 
	 * Calling merge on a transient entity will persist it and returns a managed
	 * 
	 * instance.
	 * 
	 * <strong>Unlike the JPA specs, Achilles returns the same entity passed
	 * 
	 * in parameter if the latter is in managed state. It was designed on purpose
	 * 
	 * so you do not loose the reference of the passed entity. For transient
	 * 
	 * entity, the return value is a new proxy object
	 * 
	 * </strong>
	 * 
	 * @param entity
	 *            Entity to be merged
	 * @param writeLevel
	 *            Consistency Level for write
	 * @return Merged entity or a new proxified entity
	 */
	public <T> T merge(final T entity, ConsistencyLevel writeLevel)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Merging entity '{}' with write consistency level {}",
					proxifier.unproxy(entity), writeLevel);
		}
		return this.merge(entity, Optional.fromNullable(writeLevel), Optional.<Integer> absent());

	}

	/**
	 * Merge an entity with the given time-to-live. All join entities with CascadeType.MERGE or CascadeType.ALL
	 * 
	 * will be also merged <strong>but the time-to-live will not be cascaded</strong>, updating their current state in Cassandra.
	 * 
	 * Calling merge on a transient entity will persist it and returns a managed
	 * 
	 * instance.
	 * 
	 * <strong>Unlike the JPA specs, Achilles returns the same entity passed
	 * 
	 * in parameter if the latter is in managed state. It was designed on purpose
	 * 
	 * so you do not loose the reference of the passed entity. For transient
	 * 
	 * entity, the return value is a new proxy object
	 * 
	 * </strong>
	 * 
	 * @param entity
	 *            Entity to be merged
	 * @param ttl
	 *            Time to live
	 * @return Merged entity or a new proxified entity
	 */
	public <T> T merge(final T entity, int ttl)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Merging entity '{}' with ttl {}",
					proxifier.unproxy(entity), ttl);
		}
		return this
				.merge(entity, Optional.<ConsistencyLevel> absent(), Optional.fromNullable(ttl));

	}

	/**
	 * Merge an entity with the given time-to-live and Consistency Level. All join entities with
	 * 
	 * CascadeType.MERGE or CascadeType.ALL will be also merged with the same Consistency Level
	 * 
	 * <strong>but the time-to-live will not be cascaded</strong>
	 * 
	 * Calling merge on a transient entity will persist it and returns a managed instance.
	 * 
	 * <strong>Unlike the JPA specs, Achilles returns the same entity passed
	 * 
	 * in parameter if the latter is in managed state. It was designed on purpose
	 * 
	 * so you do not loose the reference of the passed entity. For transient
	 * 
	 * entity, the return value is a new proxy object
	 * 
	 * </strong>
	 * 
	 * @param entity
	 *            Entity to be merged
	 * @param ttl
	 *            Time to live
	 * @param writeLevel
	 *            Consistency Level for write
	 * @return Merged entity or a new proxified entity
	 */
	public <T> T merge(final T entity, int ttl, ConsistencyLevel writeLevel)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Merging entity '{}' with ttl {} and consistency level {}",
					proxifier.unproxy(entity), ttl, writeLevel);
		}
		return this
				.merge(entity, Optional.fromNullable(writeLevel), Optional.fromNullable(ttl));

	}

	<T> T merge(final T entity, Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
	{
		entityValidator.validateNotWideRow(entity, entityMetaMap);
		entityValidator.validateEntity(entity, entityMetaMap);
		CONTEXT context = initPersistenceContext(entity, Optional.<ConsistencyLevel> absent(),
				writeLevelO, ttlO);
		return context.<T> merge(entity);

	}

	/**
	 * Remove an entity. Join entities are <strong>not</strong> removed.
	 * 
	 * CascadeType.REMOVE is not supported as per design to avoid
	 * 
	 * inconsistencies while removing <em>shared</em> join entities.
	 * 
	 * You need to remove the join entities manually
	 * 
	 * @param entity
	 *            Entity to be removed
	 */
	public void remove(Object entity)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Removing entity '{}'", proxifier.unproxy(entity));
		}
		this.remove(entity, Optional.<ConsistencyLevel> absent());
	}

	/**
	 * Remove an entity with the given Consistency Level for write. Join entities are <strong>not</strong> removed.
	 * 
	 * CascadeType.REMOVE is not supported as per design to avoid
	 * 
	 * inconsistencies while removing <em>shared</em> join entities.
	 * 
	 * You need to remove the join entities manually
	 * 
	 * @param entity
	 *            Entity to be removed
	 * @param writeLevel
	 *            Consistency Level for write
	 */
	public void remove(final Object entity, ConsistencyLevel writeLevel)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Removing entity '{}' with write consistency level {}",
					proxifier.unproxy(entity), writeLevel);
		}
		this.remove(entity, Optional.fromNullable(writeLevel));
	}

	void remove(final Object entity, Optional<ConsistencyLevel> writeLevelO)
	{
		entityValidator.validateEntity(entity, entityMetaMap);
		proxifier.ensureProxy(entity);
		CONTEXT context = initPersistenceContext(entity, Optional.<ConsistencyLevel> absent(),
				writeLevelO, Optional.<Integer> absent());
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
	public <T> T find(Class<T> entityClass, Object primaryKey)
	{
		log.debug("Find entity class '{}' with primary key {}", entityClass, primaryKey);
		return this.find(entityClass, primaryKey, Optional.<ConsistencyLevel> absent());
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
			ConsistencyLevel readLevel)
	{
		log.debug("Find entity class '{}' with primary key {} and read consistency level {}",
				entityClass, primaryKey, readLevel);
		return this.find(entityClass, primaryKey, Optional.fromNullable(readLevel));
	}

	<T> T find(final Class<T> entityClass, final Object primaryKey,
			Optional<ConsistencyLevel> readLevelO)
	{
		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null");
		CONTEXT context = initPersistenceContext(entityClass, primaryKey, readLevelO,
				Optional.<ConsistencyLevel> absent(), Optional.<Integer> absent());
		return context.<T> find(entityClass);
	}

	/**
	 * Find an entity. Works exactly as find(Class<T> entityClass, Object primaryKey)
	 * 
	 * @param entityClass
	 *            Entity type
	 * @param primaryKey
	 *            Primary key (Cassandra row key) of the entity to load
	 * @param entity
	 *            Found entity or null if no entity is found
	 */
	public <T> T getReference(Class<T> entityClass, Object primaryKey)
	{
		log.debug("Get reference for entity class '{}' with primary key {}", entityClass,
				primaryKey);
		return this.getReference(entityClass, primaryKey, Optional.<ConsistencyLevel> absent());
	}

	/**
	 * Find an entity with the given Consistency Level for read. Works exactly as find(Class<T> entityClass, Object primaryKey)
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
	public <T> T getReference(final Class<T> entityClass, final Object primaryKey,
			ConsistencyLevel readLevel)
	{
		log
				.debug("Get reference for entity class '{}' with primary key {} and read consistency level {}",
						entityClass, primaryKey, readLevel);
		return this.getReference(entityClass, primaryKey, Optional.fromNullable(readLevel));
	}

	<T> T getReference(final Class<T> entityClass, final Object primaryKey,
			Optional<ConsistencyLevel> readLevelO)
	{
		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null");
		CONTEXT context = initPersistenceContext(entityClass, primaryKey, readLevelO,
				Optional.<ConsistencyLevel> absent(), Optional.<Integer> absent());
		return context.<T> getReference(entityClass);
	}

	/**
	 * Refresh an entity. All join entities with CascadeType.REFRESH or CascadeType.ALL
	 * 
	 * will be also refreshed from Cassandra.
	 * 
	 * @param entity
	 *            Entity to be refreshed
	 */
	public void refresh(Object entity)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Refreshing entity '{}'", proxifier.unproxy(entity));
		}

		this.refresh(entity, Optional.<ConsistencyLevel> absent());
	}

	/**
	 * Refresh an entity with the given Consistency Level for read. All join entities with CascadeType.REFRESH or CascadeType.ALL
	 * 
	 * will be also refreshed from Cassandra.
	 * 
	 * @param entity
	 *            Entity to be refreshed
	 * @param readLevel
	 *            Consistency Level for read
	 */
	public void refresh(final Object entity, ConsistencyLevel readLevel)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Refreshing entity '{}' with read consistency level {}",
					proxifier.unproxy(entity), readLevel);
		}

		this.refresh(entity, Optional.fromNullable(readLevel));
	}

	void refresh(final Object entity, Optional<ConsistencyLevel> readLevelO)
	{
		entityValidator.validateEntity(entity, entityMetaMap);
		entityValidator.validateNotWideRow(entity, entityMetaMap);
		proxifier.ensureProxy(entity);
		CONTEXT context = initPersistenceContext(entity, readLevelO,
				Optional.<ConsistencyLevel> absent(), Optional.<Integer> absent());
		context.refresh();
	}

	/**
	 * Initialize all lazy fields of a 'managed' entity, except WideMap fields.
	 * 
	 * Raise an <strong>IllegalStateException</strong> if the entity is not 'managed'
	 * 
	 */
	public <T> void initialize(final T entity)
	{
		log.debug("Force lazy fields initialization for entity {}", entity);
		final EntityMeta entityMeta = prepareEntityForInitialization(entity);
		initializer.initializeEntity(entity, entityMeta);
	}

	/**
	 * Initialize all lazy fields of a collection of 'managed' entities, except WideMap fields.
	 * 
	 * Raise an IllegalStateException if an entity is not 'managed'
	 * 
	 */
	public <T> void initialize(Collection<T> entities)
	{
		log.debug("Force lazy fields initialization for entity collection {}", entities);
		for (T entity : entities)
		{
			EntityMeta entityMeta = prepareEntityForInitialization(entity);
			initializer.initializeEntity(entity, entityMeta);
		}
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
	public <T> T unwrap(T proxy)
	{
		log.debug("Unproxying entity {}", proxy);

		T realObject = proxifier.unproxy(proxy);

		return realObject;
	}

	/**
	 * Unwrap a collection of 'managed' entities to prepare them for serialization
	 * 
	 * See {@link #unwrap}
	 * 
	 * @param proxy
	 *            collection
	 * @return real object collection
	 */
	public <T> Collection<T> unwrap(Collection<T> proxies)
	{
		log.debug("Unproxying collection of entities {}", proxies);

		return proxifier.unproxy(proxies);
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
	public <T> List<T> unwrap(List<T> proxies)
	{
		log.debug("Unproxying list of entities {}", proxies);

		return proxifier.unproxy(proxies);
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
	public <T> Set<T> unwrap(Set<T> proxies)
	{
		log.debug("Unproxying set of entities {}", proxies);

		return proxifier.unproxy(proxies);
	}

	protected abstract CONTEXT initPersistenceContext(Object entity,
			Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO,
			Optional<Integer> ttl);

	protected abstract CONTEXT initPersistenceContext(Class<?> entityClass, Object primaryKey,
			Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO,
			Optional<Integer> ttl);

	private EntityMeta prepareEntityForInitialization(Object entity)
	{
		proxifier.ensureProxy(entity);
		Object realObject = proxifier.getRealObject(entity);
		EntityMeta entityMeta = entityMetaMap.get(realObject.getClass());
		return entityMeta;
	}

	protected Map<Class<?>, EntityMeta> getEntityMetaMap()
	{
		return entityMetaMap;
	}

	protected AchillesConsistencyLevelPolicy getConsistencyPolicy()
	{
		return consistencyPolicy;
	}

	protected ConfigurationContext getConfigContext()
	{
		return configContext;
	}

	protected void setProxifier(EntityProxifier<CONTEXT> proxifier)
	{
		this.proxifier = proxifier;
	}

	protected void setEntityValidator(EntityValidator<CONTEXT> entityValidator)
	{
		this.entityValidator = entityValidator;
	}

	protected void setInitializer(EntityInitializer initializer)
	{
		this.initializer = initializer;
	}

	protected void setEntityMetaMap(Map<Class<?>, EntityMeta> entityMetaMap)
	{
		this.entityMetaMap = entityMetaMap;
	}

	protected void setConsistencyPolicy(AchillesConsistencyLevelPolicy consistencyPolicy)
	{
		this.consistencyPolicy = consistencyPolicy;
	}

	protected void setConfigContext(ConfigurationContext configContext)
	{
		this.configContext = configContext;
	}

}
