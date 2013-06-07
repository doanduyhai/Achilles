package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftBatchingFlushContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.execution.ThriftSafeExecutionContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftBatchingEntityManager
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftBatchingEntityManager extends ThriftEntityManager
{
	private static final Logger log = LoggerFactory.getLogger(ThriftBatchingEntityManager.class);

	private ThriftBatchingFlushContext flushContext;

	ThriftBatchingEntityManager(AchillesEntityManagerFactory entityManagerFactory,
			Map<Class<?>, EntityMeta> entityMetaMap, ThriftDaoContext thriftDaoContext,
			ConfigurationContext configContext)
	{
		super(entityManagerFactory, entityMetaMap, thriftDaoContext, configContext);
		this.flushContext = new ThriftBatchingFlushContext(thriftDaoContext, consistencyPolicy);
	}

	/**
	 * Start a batch session using a Hector mutator.
	 */
	public void startBatch()
	{
		log.debug("Starting batch mode");
		flushContext.startBatch();
	}

	/**
	 * Start a batch session with read/write consistency levels using a Hector mutator.
	 */
	public void startBatch(ConsistencyLevel readLevel, ConsistencyLevel writeLevel)
	{
		log.debug("Starting batch mode with write consistency level {}", writeLevel.name());
		startBatch();
		if (readLevel != null)
		{
			flushContext.setReadConsistencyLevel(readLevel);
		}
		if (writeLevel != null)
		{
			flushContext.setWriteConsistencyLevel(writeLevel);
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
	public void endBatch()
	{
		log.debug("Ending batch mode");
		flushContext.endBatch();
	}

	/**
	 * Cleaning all pending mutations for the current batch session.
	 */
	public void cleanBatch()
	{
		log.debug("Cleaning all pending mutations");
		flushContext.cleanUp();
	}

	@Override
	public void persist(final Object entity)
	{
		reinitConsistencyLevelsOnError(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				ThriftBatchingEntityManager.super.persist(entity);
				return null;
			}
		});
	}

	@Override
	public void persist(final Object entity, ConsistencyLevel writeLevel)
	{
		flushContext.cleanUp();
		throw new AchillesException(
				"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
	}

	@Override
	public <T> T merge(final T entity)
	{
		return reinitConsistencyLevelsOnError(new ThriftSafeExecutionContext<T>()
		{
			@Override
			public T execute()
			{
				return ThriftBatchingEntityManager.super.merge(entity);
			}
		});
	}

	@Override
	public <T> T merge(final T entity, ConsistencyLevel writeLevel)
	{
		flushContext.cleanUp();
		throw new AchillesException(
				"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
	}

	@Override
	public void remove(final Object entity)
	{
		reinitConsistencyLevelsOnError(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				ThriftBatchingEntityManager.super.remove(entity);
				return null;
			}
		});
	}

	@Override
	public void remove(final Object entity, ConsistencyLevel writeLevel)
	{
		flushContext.cleanUp();
		throw new AchillesException(
				"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
	}

	@Override
	public <T> T find(final Class<T> entityClass, final Object primaryKey)
	{
		return reinitConsistencyLevelsOnError(new ThriftSafeExecutionContext<T>()
		{
			@Override
			public T execute()
			{
				return ThriftBatchingEntityManager.super.find(entityClass, primaryKey);
			}
		});
	}

	@Override
	public <T> T find(final Class<T> entityClass, final Object primaryKey,
			ConsistencyLevel readLevel)
	{
		flushContext.cleanUp();
		throw new AchillesException(
				"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
	}

	@Override
	public <T> T getReference(final Class<T> entityClass, final Object primaryKey)
	{
		return reinitConsistencyLevelsOnError(new ThriftSafeExecutionContext<T>()
		{
			@Override
			public T execute()
			{
				return ThriftBatchingEntityManager.super.find(entityClass, primaryKey);
			}
		});
	}

	@Override
	public <T> T getReference(final Class<T> entityClass, final Object primaryKey,
			ConsistencyLevel readLevel)
	{
		flushContext.cleanUp();
		throw new AchillesException(
				"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
	}

	@Override
	public void refresh(final Object entity)
	{
		reinitConsistencyLevelsOnError(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				ThriftBatchingEntityManager.super.refresh(entity);
				return null;
			}
		});
	}

	@Override
	public <T> void initialize(final T entity)
	{
		reinitConsistencyLevelsOnError(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				ThriftBatchingEntityManager.super.initialize(entity);
				return null;
			}
		});
	}

	@Override
	public <T> void initialize(final Collection<T> entities)
	{
		reinitConsistencyLevelsOnError(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				ThriftBatchingEntityManager.super.initialize(entities);
				return null;
			}
		});
	}

	@Override
	public void refresh(final Object entity, ConsistencyLevel readLevel)
	{
		throw new AchillesException(
				"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
	}

	protected ThriftPersistenceContext initPersistenceContext(Class<?> entityClass,
			Object primaryKey)
	{
		log.trace("Initializing new persistence context for entity class {} and primary key {}",
				entityClass.getCanonicalName(), primaryKey);

		EntityMeta entityMeta = entityMetaMap.get(entityClass);
		return new ThriftPersistenceContext(entityMeta, configContext, thriftDaoContext,
				flushContext, entityClass, primaryKey);
	}

	protected ThriftPersistenceContext initPersistenceContext(Object entity)
	{
		log.trace("Initializing new persistence context for entity {}", entity);

		EntityMeta entityMeta = this.entityMetaMap.get(proxifier.deriveBaseClass(entity));
		return new ThriftPersistenceContext(entityMeta, configContext, thriftDaoContext,
				flushContext, entity);
	}

	private <T> T reinitConsistencyLevelsOnError(ThriftSafeExecutionContext<T> context)
	{
		try
		{
			return context.execute();
		}
		catch (Exception e)
		{
			this.flushContext.cleanUp();
			throw new AchillesException(e);
		}
	}
}
