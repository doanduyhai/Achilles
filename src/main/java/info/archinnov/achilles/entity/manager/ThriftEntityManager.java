package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.context.AbstractBatchContext;
import info.archinnov.achilles.entity.context.AbstractBatchContext.BatchType;
import info.archinnov.achilles.entity.context.BatchContext;
import info.archinnov.achilles.entity.context.NoBatchContext;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.execution_context.SafeExecutionContext;
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

	private final Map<Class<?>, EntityMeta<?>> entityMetaMap;
	private final Map<String, GenericEntityDao<?>> entityDaosMap;
	private final Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap;
	private final CounterDao counterDao;
	private AchillesConfigurableConsistencyLevelPolicy consistencyPolicy;
	private AbstractBatchContext batchContext;

	private EntityPersister persister = new EntityPersister();
	private EntityLoader loader = new EntityLoader();
	private EntityMerger merger = new EntityMerger();
	private EntityRefresher refresher = new EntityRefresher();
	private EntityInitializer initializer = new EntityInitializer();
	private EntityProxifier proxifier = new EntityProxifier();
	private EntityValidator entityValidator = new EntityValidator();

	ThriftEntityManager(Map<Class<?>, EntityMeta<?>> entityMetaMap, //
			Map<String, GenericEntityDao<?>> entityDaosMap, //
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap,//
			CounterDao counterDao, //
			AchillesConfigurableConsistencyLevelPolicy consistencyPolicy)
	{
		this.entityMetaMap = entityMetaMap;
		this.entityDaosMap = entityDaosMap;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.counterDao = counterDao;
		this.consistencyPolicy = consistencyPolicy;
		this.batchContext = new NoBatchContext(entityDaosMap, columnFamilyDaosMap, counterDao);
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
		entityValidator.validateNotCFDirectMapping(entity, entityMetaMap);

		if (proxifier.isProxy(entity))
		{
			throw new IllegalStateException(
					"Then entity is already in 'managed' state. Please use the merge() method instead of persist()");
		}
		final PersistenceContext<?> context = initPersistenceContext(entity);
		reinitBatchOnlyOnError(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				persister.persist(context);
				context.flush();
				return null;
			}
		});

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
	public void persist(Object entity, ConsistencyLevel writeLevel)
	{
		entityValidator.validateNoPendingBatch(batchContext);
		batchContext.setWriteConsistencyLevel(writeLevel);
		persist(entity);
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
	@Override
	public <T> T merge(T entity)
	{
		entityValidator.validateEntity(entity, entityMetaMap);
		final PersistenceContext<?> context = initPersistenceContext(entity);

		return reinitBatchOnlyOnError(new SafeExecutionContext<T>()
		{
			@Override
			public T execute()
			{
				T merged = merger.mergeEntity(context);
				context.flush();
				return merged;
			}
		});
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
	public <T> T merge(T entity, ConsistencyLevel writeLevel)
	{
		entityValidator.validateNoPendingBatch(batchContext);
		batchContext.setWriteConsistencyLevel(writeLevel);
		return merge(entity);
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
		entityValidator.validateEntity(entity, entityMetaMap);
		proxifier.ensureProxy(entity);
		final PersistenceContext<?> context = initPersistenceContext(entity);

		reinitBatchOnlyOnError(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				persister.remove(context);
				context.flush();
				return null;
			}
		});
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
	public void remove(Object entity, ConsistencyLevel writeLevel)
	{
		entityValidator.validateNoPendingBatch(batchContext);
		batchContext.setWriteConsistencyLevel(writeLevel);
		remove(entity);
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
	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey)
	{
		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null");

		final PersistenceContext<Object> context = initPersistenceContext(entityClass, primaryKey);

		return reinitBatchOnlyOnError(new SafeExecutionContext<T>()
		{
			@Override
			public T execute()
			{
				T entity = loader.load(context);
				if (entity != null)
				{
					entity = proxifier.buildProxy(entity, context);
				}
				return entity;
			}
		});
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
	public <T> T find(Class<T> entityClass, Object primaryKey, ConsistencyLevel readLevel)
	{
		entityValidator.validateNoPendingBatch(batchContext);
		batchContext.setReadConsistencyLevel(readLevel);
		return find(entityClass, primaryKey);
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
	public <T> T getReference(Class<T> entityClass, Object primaryKey, ConsistencyLevel readLevel)
	{
		entityValidator.validateNoPendingBatch(batchContext);
		batchContext.setReadConsistencyLevel(readLevel);
		return find(entityClass, primaryKey);
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
		entityValidator.validateEntity(entity, entityMetaMap);
		entityValidator.validateNotCFDirectMapping(entity, entityMetaMap);
		proxifier.ensureProxy(entity);
		final PersistenceContext<?> context = initPersistenceContext(entity);

		reinitBatchOnlyOnError(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				refresher.refresh(context);
				return null;
			}
		});
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
	public void refresh(Object entity, ConsistencyLevel readLevel)
	{
		entityValidator.validateNoPendingBatch(batchContext);
		batchContext.setReadConsistencyLevel(readLevel);
		refresh(entity);
	}

	/**
	 * Start a batch session using a Hector mutator.
	 * 
	 * Throw <strong>IllegalStateException</strong> if there is already a pending batch
	 */
	public void startBatch()
	{
		if (batchContext.type() == BatchType.BATCH)
		{
			throw new IllegalStateException(
					"There is already a pending batch for this Entity Manager");
		}
		else
		{
			batchContext = new BatchContext(entityDaosMap, columnFamilyDaosMap, counterDao);
		}
	}

	public void startBatch(ConsistencyLevel readLevel, ConsistencyLevel writeLevel)
	{
		startBatch();
		if (readLevel != null)
		{
			batchContext.setReadConsistencyLevel(readLevel);
		}
		if (writeLevel != null)
		{
			batchContext.setWriteConsistencyLevel(writeLevel);
		}
	}

	/**
	 * End an existing batch and flush all the mutators.
	 * 
	 * All join entities will be flushed through their own mutator.
	 * 
	 * Do nothing if no batch mutator was started
	 * 
	 */
	public <T, ID> void endBatch()
	{
		if (batchContext.type() == BatchType.NONE)
		{
			throw new IllegalStateException(
					"There is no pending batch for this Entity Manager. Please start a batch first");
		}
		else
		{
			alwaysReinitBatch(new SafeExecutionContext<Void>()
			{

				@Override
				public Void execute()
				{
					batchContext.endBatch();
					return null;
				}
			});
		}
	}

	/**
	 * Initialize all lazy fields of a 'managed' entity, except WideMap fields.
	 * 
	 * Raise an <strong>IllegalStateException</strong> if the entity is not 'managed'
	 * 
	 */
	public <T> void initialize(final T entity)
	{
		final EntityMeta<?> entityMeta = prepareEntityForInitialization(entity);
		reinitBatchOnlyOnError(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				initializer.initializeEntity(entity, entityMeta);
				return null;
			}
		});
	}

	/**
	 * Initialize all lazy fields of a collection of 'managed' entities, except WideMap fields.
	 * 
	 * Raise an IllegalStateException if an entity is not 'managed'
	 * 
	 */
	public <T> void initialize(Collection<T> entities)
	{
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
		return proxifier.unproxy(proxies);
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
	private <T, ID> PersistenceContext<ID> initPersistenceContext(Class<T> entityClass,
			ID primaryKey)
	{
		EntityMeta<ID> entityMeta = (EntityMeta<ID>) this.entityMetaMap.get(entityClass);
		return new PersistenceContext<ID>(entityMeta, entityDaosMap, columnFamilyDaosMap,
				counterDao, consistencyPolicy, batchContext, entityClass, primaryKey);
	}

	@SuppressWarnings("unchecked")
	private <ID> PersistenceContext<ID> initPersistenceContext(Object entity)
	{
		EntityMeta<ID> entityMeta = (EntityMeta<ID>) this.entityMetaMap.get(proxifier
				.deriveBaseClass(entity));
		return new PersistenceContext<ID>(entityMeta, entityDaosMap, columnFamilyDaosMap,
				counterDao, consistencyPolicy, batchContext, entity);
	}

	private <T> EntityMeta<?> prepareEntityForInitialization(T entity)
	{
		proxifier.ensureProxy(entity);
		Object realObject = proxifier.getRealObject(entity);
		EntityMeta<?> entityMeta = entityMetaMap.get(realObject.getClass());
		return entityMeta;
	}

	private <T> T reinitBatchOnlyOnError(SafeExecutionContext<T> context)
	{
		try
		{
			return context.execute();
		}
		catch (RuntimeException e)
		{
			reinitBatchContext();
			throw e;
		}
		catch (Error err)
		{
			reinitBatchContext();
			throw err;
		}
		finally
		{
			batchContext.reinitConsistencyLevels();
		}
	}

	private <T> T alwaysReinitBatch(SafeExecutionContext<T> context)
	{
		try
		{
			return context.execute();
		}
		finally
		{
			reinitBatchContext();
			batchContext.reinitConsistencyLevels();
		}
	}

	private void reinitBatchContext()
	{
		batchContext = new NoBatchContext(entityDaosMap, columnFamilyDaosMap, counterDao);
	}
}
