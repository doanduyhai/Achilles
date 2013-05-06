package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.entity.context.ConfigurationContext;
import info.archinnov.achilles.entity.context.DaoContext;
import info.archinnov.achilles.entity.context.ImmediateFlushContext;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.context.execution.SafeExecutionContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftEntityManager
 * 
 * Thrift-based Entity Manager for Achilles. This entity manager is perfectly thread-safe and
 * 
 * can be used as a singleton. Entity state is stored in proxy object, which is obviously not
 * 
 * thread-safe.
 * 
 * Internally the ThriftEntityManager relies on Hector API for common operations
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityManager implements EntityManager
{
	private static final Logger log = LoggerFactory.getLogger(ThriftEntityManager.class);

	protected final Map<Class<?>, EntityMeta<?>> entityMetaMap;
	protected final AchillesConfigurableConsistencyLevelPolicy consistencyPolicy;

	protected final ConfigurationContext configContext;
	protected final DaoContext daoContext;

	protected EntityPersister persister = new EntityPersister();
	protected EntityLoader loader = new EntityLoader();
	protected EntityMerger merger = new EntityMerger();
	protected EntityRefresher refresher = new EntityRefresher();
	protected EntityInitializer initializer = new EntityInitializer();
	protected EntityProxifier proxifier = new EntityProxifier();
	protected EntityValidator entityValidator = new EntityValidator();

	ThriftEntityManager(Map<Class<?>, EntityMeta<?>> entityMetaMap, //
			DaoContext daoContext, //
			ConfigurationContext configContext)
	{
		this.entityMetaMap = entityMetaMap;
		this.daoContext = daoContext;
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
	@Override
	public void persist(Object entity)
	{
		log.debug("Persisting entity '{}'", entity);
		entityValidator.validateEntity(entity, entityMetaMap);
		entityValidator.validateNotWideRow(entity, entityMetaMap);

		if (proxifier.isProxy(entity))
		{
			throw new IllegalStateException(
					"Then entity is already in 'managed' state. Please use the merge() method instead of persist()");
		}
		PersistenceContext<?> context = initPersistenceContext(entity);
		persister.persist(context);
		context.flush();
	}

	/**
	 * Persist an entity with the given Consistency Level for write. All join entities with CascadeType.PERSIST or CascadeType.ALL
	 * 
	 * will be also persisted, overriding their current state in Cassandra
	 * 
	 * @param entity
	 *            Entity to be persisted
	 * @param writeLevel
	 *            Consistency Level for write
	 */
	public void persist(final Object entity, ConsistencyLevel writeLevel)
	{
		log.debug("Persisting entity '{}' with write consistency level {}", entity,
				writeLevel.name());
		consistencyPolicy.setCurrentWriteLevel(writeLevel);
		reinitConsistencyLevels(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				persist(entity);
				return null;
			}
		});
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
	@SuppressWarnings("unchecked")
	@Override
	public <T> T merge(T entity)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Merging entity '{}' ", proxifier.unproxy(entity));
		}
		entityValidator.validateNotWideRow(entity, entityMetaMap);
		entityValidator.validateEntity(entity, entityMetaMap);
		PersistenceContext<?> context = initPersistenceContext(entity);
		T merged = (T) merger.mergeEntity(context);
		context.flush();
		return merged;
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
					proxifier.unproxy(entity), writeLevel.name());
		}
		consistencyPolicy.setCurrentWriteLevel(writeLevel);
		return reinitConsistencyLevels(new SafeExecutionContext<T>()
		{
			@Override
			public T execute()
			{
				return merge(entity);
			}
		});
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
	@Override
	public void remove(Object entity)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Removing entity '{}'", proxifier.unproxy(entity));
		}
		entityValidator.validateEntity(entity, entityMetaMap);
		proxifier.ensureProxy(entity);
		PersistenceContext<?> context = initPersistenceContext(entity);
		persister.remove(context);
		context.flush();

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
					proxifier.unproxy(entity), writeLevel.name());
		}
		consistencyPolicy.setCurrentWriteLevel(writeLevel);
		reinitConsistencyLevels(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				remove(entity);
				return null;
			}
		});

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
	@SuppressWarnings("unchecked")
	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey)
	{
		log.debug("Find entity class '{}' with primary key {}", entityClass, primaryKey);

		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null");
		PersistenceContext<Object> context = initPersistenceContext(entityClass, primaryKey);

		T entity = (T) loader.load(context);
		if (entity != null)
		{
			entity = proxifier.buildProxy(entity, context);
		}
		return entity;
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
				entityClass, primaryKey, readLevel.name());

		consistencyPolicy.setCurrentReadLevel(readLevel);
		return reinitConsistencyLevels(new SafeExecutionContext<T>()
		{
			@Override
			public T execute()
			{
				return find(entityClass, primaryKey);
			}
		});
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
	@Override
	public <T> T getReference(Class<T> entityClass, Object primaryKey)
	{
		log.debug("Get reference for entity class '{}' with primary key {}", entityClass,
				primaryKey);

		return find(entityClass, primaryKey);
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
		log.debug(
				"Get reference for entity class '{}' with primary key {} and read consistency level {}",
				entityClass, primaryKey, readLevel.name());

		consistencyPolicy.setCurrentReadLevel(readLevel);
		return reinitConsistencyLevels(new SafeExecutionContext<T>()
		{
			@Override
			public T execute()
			{
				return find(entityClass, primaryKey);
			}
		});
	}

	/**
	 * Refresh an entity. All join entities with CascadeType.REFRESH or CascadeType.ALL
	 * 
	 * will be also refreshed from Cassandra.
	 * 
	 * @param entity
	 *            Entity to be refreshed
	 */
	@Override
	public void refresh(Object entity)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Refreshing entity '{}'", proxifier.unproxy(entity));
		}
		entityValidator.validateEntity(entity, entityMetaMap);
		entityValidator.validateNotWideRow(entity, entityMetaMap);
		proxifier.ensureProxy(entity);
		PersistenceContext<?> context = initPersistenceContext(entity);
		refresher.refresh(context);
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
					proxifier.unproxy(entity), readLevel.name());
		}
		consistencyPolicy.setCurrentReadLevel(readLevel);
		reinitConsistencyLevels(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				refresh(entity);
				return null;
			}
		});
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
		final EntityMeta<?> entityMeta = prepareEntityForInitialization(entity);
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
			EntityMeta<?> entityMeta = prepareEntityForInitialization(entity);
			initializer.initializeEntity(entity, entityMeta);
		}
	}

	/**
	 * Unproxy a 'managed' entity to prepare it for serialization
	 * 
	 * Raise an IllegalStateException if the entity is not managed'
	 * 
	 * @param proxy
	 * @return real object
	 */
	public <T> T unproxy(T proxy)
	{
		log.debug("Unproxying entity {}", proxy);

		T realObject = this.proxifier.unproxy(proxy);

		return realObject;
	}

	/**
	 * Unproxy a collection of 'managed' entities to prepare them for serialization
	 * 
	 * Raise an IllegalStateException if an entity is not managed' in the collection
	 * 
	 * @param proxy
	 *            collection
	 * @return real object collection
	 */
	public <T> Collection<T> unproxy(Collection<T> proxies)
	{
		log.debug("Unproxying collection of entities {}", proxies);

		return proxifier.unproxy(proxies);
	}

	/**
	 * Unproxy a list of 'managed' entities to prepare them for serialization
	 * 
	 * Raise an IllegalStateException if an entity is not managed' in the list
	 * 
	 * @param proxy
	 *            list
	 * @return real object list
	 */
	public <T> List<T> unproxy(List<T> proxies)
	{
		log.debug("Unproxying list of entities {}", proxies);

		return proxifier.unproxy(proxies);
	}

	/**
	 * Unproxy a set of 'managed' entities to prepare them for serialization
	 * 
	 * Raise an IllegalStateException if an entity is not managed' in the set
	 * 
	 * @param proxy
	 *            set
	 * @return real object set
	 */
	public <T> Set<T> unproxy(Set<T> proxies)
	{
		log.debug("Unproxying set of entities {}", proxies);

		return proxifier.unproxy(proxies);
	}

	/**
	 * Create a new state-full EntityManager for batch handling <br/>
	 * <br/>
	 * 
	 * <strong>WARNING : This EntityManager is state-full and not thread-safe. In case of exception, you MUST not re-use it but create another one</strong>
	 * 
	 * @return a new state-full EntityManager
	 */
	public ThriftBatchingEntityManager batchingEntityManager()
	{
		return new ThriftBatchingEntityManager(entityMetaMap, daoContext, configContext);
	}

	/**
	 * Not supported operation. Will throw UnsupportedOperationException
	 */
	@Override
	public void flush()
	{
		throw new UnsupportedOperationException("This operation is not supported for Cassandra");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public void setFlushMode(FlushModeType flushMode)
	{
		throw new UnsupportedOperationException("This operation is not supported for Cassandra");

	}

	/**
	 * Always return FlushModeType.AUTO
	 */
	@Override
	public FlushModeType getFlushMode()
	{
		return FlushModeType.AUTO;
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public void lock(Object entity, LockModeType lockMode)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public void clear()
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public boolean contains(Object entity)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public Query createQuery(String qlString)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public Query createNamedQuery(String name)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Entity Manager");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public Query createNativeQuery(String sqlString)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public Query createNativeQuery(String sqlString, Class resultClass)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public Query createNativeQuery(String sqlString, String resultSetMapping)
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public void joinTransaction()
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");

	}

	/**
	 * @return the ThriftEntityManager instance itself
	 */
	@Override
	public Object getDelegate()
	{
		return this;
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public void close()
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");

	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public boolean isOpen()
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");
	}

	/**
	 * Not supported operation. Will throw <strong>UnsupportedOperationException</strong>
	 */
	@Override
	public EntityTransaction getTransaction()
	{
		throw new UnsupportedOperationException(
				"This operation is not supported for this Cassandra");
	}

	@SuppressWarnings("unchecked")
	protected <T, ID> PersistenceContext<ID> initPersistenceContext(Class<T> entityClass,
			ID primaryKey)
	{
		log.trace("Initializing new persistence context for entity class {} and primary key {}",
				entityClass.getCanonicalName(), primaryKey);

		EntityMeta<ID> entityMeta = (EntityMeta<ID>) this.entityMetaMap.get(entityClass);
		return new PersistenceContext<ID>(entityMeta, configContext, daoContext,
				new ImmediateFlushContext(daoContext, consistencyPolicy), entityClass, primaryKey);
	}

	@SuppressWarnings("unchecked")
	protected <ID> PersistenceContext<ID> initPersistenceContext(Object entity)
	{
		log.trace("Initializing new persistence context for entity {}", entity);

		EntityMeta<ID> entityMeta = (EntityMeta<ID>) this.entityMetaMap.get(proxifier
				.deriveBaseClass(entity));
		return new PersistenceContext<ID>(entityMeta, configContext, daoContext,
				new ImmediateFlushContext(daoContext, consistencyPolicy), entity);
	}

	private <T> EntityMeta<?> prepareEntityForInitialization(T entity)
	{
		proxifier.ensureProxy(entity);
		Object realObject = proxifier.getRealObject(entity);
		EntityMeta<?> entityMeta = entityMetaMap.get(realObject.getClass());
		return entityMeta;
	}

	private <T> T reinitConsistencyLevels(SafeExecutionContext<T> context)
	{
		try
		{
			return context.execute();
		}
		finally
		{
			consistencyPolicy.reinitCurrentConsistencyLevels();
			consistencyPolicy.reinitDefaultConsistencyLevels();
		}
	}
}
