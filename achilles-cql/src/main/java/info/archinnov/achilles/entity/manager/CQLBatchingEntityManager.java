package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.context.CQLBatchingFlushContext;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.CQLPersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;

/**
 * CQLBatchingEntityManager
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLBatchingEntityManager extends CQLEntityManager {

    private static final Logger log = LoggerFactory.getLogger(CQLBatchingEntityManager.class);

    private CQLBatchingFlushContext flushContext;

    CQLBatchingEntityManager(Map<Class<?>, EntityMeta> entityMetaMap,
            CQLPersistenceContextFactory contextFactory, CQLDaoContext daoContext, ConfigurationContext configContext) {
        super(entityMetaMap, contextFactory, daoContext, configContext);
        this.flushContext = new CQLBatchingFlushContext(daoContext, NO_CONSISTENCY_LEVEL, NO_CONSISTENCY_LEVEL,
                NO_TTL);
    }

    /**
     * Start a batch session.
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
        flushContext.setReadConsistencyLevel(Optional.fromNullable(readLevel));
        flushContext.setWriteConsistencyLevel(Optional.fromNullable(writeLevel));

    }

    /**
     * End an existing batch and flush all the pending statements.
     * 
     * All join entities will be flushed too.
     * 
     * Do nothing if there is no pending statement
     * 
     */
    public void endBatch()
    {
        log.debug("Ending batch mode");
        try {
            flushContext.endBatch();
        } finally
        {
            flushContext.cleanUp();
        }
    }

    /**
     * Cleaning all pending statements for the current batch session.
     */
    public void cleanBatch()
    {
        log.debug("Cleaning all pending statements");
        flushContext.cleanUp();
    }

    @Override
    public void persist(final Object entity, ConsistencyLevel writeLevel)
    {
        flushContext.cleanUp();
        throw new AchillesException(
                "Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
    }

    @Override
    public void persist(final Object entity, int ttl, ConsistencyLevel writeLevel)
    {
        flushContext.cleanUp();
        throw new AchillesException(
                "Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
    }

    @Override
    public <T> T merge(final T entity, ConsistencyLevel writeLevel)
    {
        flushContext.cleanUp();
        throw new AchillesException(
                "Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
    }

    @Override
    public <T> T merge(final T entity, int ttl, ConsistencyLevel writeLevel)
    {
        flushContext.cleanUp();
        throw new AchillesException(
                "Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
    }

    @Override
    public void remove(final Object entity, ConsistencyLevel writeLevel)
    {
        flushContext.cleanUp();
        throw new AchillesException(
                "Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
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
    public <T> T getReference(final Class<T> entityClass, final Object primaryKey,
            ConsistencyLevel readLevel)
    {
        flushContext.cleanUp();
        throw new AchillesException(
                "Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
    }

    @Override
    public void refresh(final Object entity, ConsistencyLevel readLevel)
    {
        throw new AchillesException(
                "Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
    }

    @Override
    protected CQLPersistenceContext initPersistenceContext(Class<?> entityClass,
            Object primaryKey, Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
    {
        log.trace("Initializing new persistence context for entity class {} and primary key {}",
                entityClass.getCanonicalName(), primaryKey);

        EntityMeta entityMeta = entityMetaMap.get(entityClass);
        return new CQLPersistenceContext(entityMeta, configContext, daoContext,
                flushContext, entityClass, primaryKey, new HashSet<String>());
    }

    @Override
    protected CQLPersistenceContext initPersistenceContext(Object entity,
            Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO)
    {
        log.trace("Initializing new persistence context for entity {}", entity);

        EntityMeta entityMeta = this.entityMetaMap.get(proxifier.deriveBaseClass(entity));
        return new CQLPersistenceContext(entityMeta, configContext, daoContext,
                flushContext, entity, new HashSet<String>());
    }
}
