package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.context.BatchingFlushContext;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchingThriftEntityManager
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftBatchingEntityManager extends ThriftEntityManager
{
	private static final Logger log = LoggerFactory.getLogger(ThriftBatchingEntityManager.class);

	private BatchingFlushContext flushContext;

	ThriftBatchingEntityManager(Map<Class<?>, EntityMeta<?>> entityMetaMap,
			Map<String, GenericEntityDao<?>> entityDaosMap,
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao,
			AchillesConfigurableConsistencyLevelPolicy consistencyPolicy)
	{
		super(entityMetaMap, entityDaosMap, columnFamilyDaosMap, counterDao, consistencyPolicy);
		this.flushContext = new BatchingFlushContext(entityDaosMap, columnFamilyDaosMap,
				counterDao, consistencyPolicy);
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
	public <T, ID> void endBatch()
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

	@SuppressWarnings("unchecked")
	protected <T, ID> PersistenceContext<ID> initPersistenceContext(Class<T> entityClass,
			ID primaryKey)
	{
		log.trace("Initializing new persistence context for entity class {} and primary key {}",
				entityClass.getCanonicalName(), primaryKey);

		EntityMeta<ID> entityMeta = (EntityMeta<ID>) entityMetaMap.get(entityClass);
		return new PersistenceContext<ID>(entityMeta, entityDaosMap, columnFamilyDaosMap,
				counterDao, consistencyPolicy, flushContext, entityClass, primaryKey);
	}

	@SuppressWarnings("unchecked")
	protected <ID> PersistenceContext<ID> initPersistenceContext(Object entity)
	{
		log.trace("Initializing new persistence context for entity {}", entity);

		EntityMeta<ID> entityMeta = (EntityMeta<ID>) this.entityMetaMap.get(proxifier
				.deriveBaseClass(entity));
		return new PersistenceContext<ID>(entityMeta, entityDaosMap, columnFamilyDaosMap,
				counterDao, consistencyPolicy, flushContext, entity);
	}
}
