package info.archinnov.achilles.context;

import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityMerger;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.HashSet;
import java.util.Set;
import me.prettyprint.hector.api.mutation.Mutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftPersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPersistenceContext extends PersistenceContext
{
    private static final Logger log = LoggerFactory.getLogger(ThriftPersistenceContext.class);

    private ThriftEntityPersister persister = new ThriftEntityPersister();
    private ThriftEntityLoader loader = new ThriftEntityLoader();
    private ThriftEntityMerger merger = new ThriftEntityMerger();
    private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
    private EntityRefresher<ThriftPersistenceContext> refresher;

    private ThriftDaoContext daoContext;
    private ThriftGenericEntityDao entityDao;
    private ThriftGenericWideRowDao wideRowDao;
    private ThriftAbstractFlushContext<?> flushContext;

    public ThriftPersistenceContext(EntityMeta entityMeta, //
            ConfigurationContext configContext, //
            ThriftDaoContext daoContext, //
            ThriftAbstractFlushContext<?> flushContext, //
            Object entity, Set<String> entitiesIdentity)
    {
        super(entityMeta, configContext, entity, flushContext, entitiesIdentity);
        log.trace("Create new persistence context for instance {} of class {}", entity,
                entityMeta.getClassName());

        initCollaborators(daoContext, flushContext);
        initDaos();
    }

    public ThriftPersistenceContext(EntityMeta entityMeta, //
            ConfigurationContext configContext, //
            ThriftDaoContext daoContext, //
            ThriftAbstractFlushContext<?> flushContext, //
            Class<?> entityClass, Object primaryKey, Set<String> entitiesIdentity)
    {
        super(entityMeta, configContext, entityClass, primaryKey, flushContext, entitiesIdentity);
        log.trace("Create new persistence context for instance {} of class {}", entity,
                entityClass.getCanonicalName());

        initCollaborators(daoContext, flushContext);
        initDaos();
    }

    private void initCollaborators(ThriftDaoContext thriftDaoContext, //
            ThriftAbstractFlushContext<?> flushContext)
    {
        refresher = new EntityRefresher<ThriftPersistenceContext>(loader, proxifier);
        this.daoContext = thriftDaoContext;
        this.flushContext = flushContext;
    }

    private void initDaos()
    {
        String tableName = entityMeta.getTableName();
        if (entityMeta.isClusteredEntity())
        {
            this.wideRowDao = daoContext.findWideRowDao(tableName);
        }
        else
        {
            this.entityDao = daoContext.findEntityDao(tableName);
        }
    }

    @Override
    public ThriftPersistenceContext createContextForJoin(EntityMeta joinMeta, Object joinEntity)
    {
        log.trace("Spawn new persistence context for instance {} of join class {}", joinEntity,
                joinMeta.getClassName());

        return new ThriftPersistenceContext(joinMeta, configContext, daoContext,
                flushContext.duplicateWithoutTtl(), joinEntity, entitiesIdentity);
    }

    @Override
    public ThriftPersistenceContext createContextForJoin(Class<?> entityClass,
            EntityMeta joinMeta, Object joinId)
    {
        log.trace("Spawn new persistence context for primary key {} of join class {}", joinId,
                joinMeta.getClassName());

        return new ThriftPersistenceContext(joinMeta, configContext, daoContext,
                flushContext.duplicateWithoutTtl(), entityClass, joinId, entitiesIdentity);
    }

    @Override
    public ThriftPersistenceContext duplicate(Object entity)
    {
        return new ThriftPersistenceContext(entityMeta, configContext, daoContext,
                flushContext.duplicateWithoutTtl(), entity, new HashSet<String>());
    }

    @Override
    public void persist()
    {
        flushContext.getConsistencyContext().executeWithWriteConsistencyLevel(
                new SafeExecutionContext<Void>()
                {
                    @Override
                    public Void execute()
                    {
                        persister.persist(ThriftPersistenceContext.this);
                        flush();
                        return null;
                    }
                });
    }

    @Override
    public <T> T merge(final T entity)
    {

        return flushContext.getConsistencyContext().executeWithWriteConsistencyLevel(
                new SafeExecutionContext<T>()
                {
                    @Override
                    public T execute()
                    {
                        T merged = merger.<T> merge(ThriftPersistenceContext.this, entity);
                        flush();
                        return merged;
                    }
                });

    }

    @Override
    public void remove()
    {
        flushContext.getConsistencyContext().executeWithWriteConsistencyLevel(
                new SafeExecutionContext<Void>()
                {
                    @Override
                    public Void execute()
                    {
                        persister.remove(ThriftPersistenceContext.this);
                        flush();
                        return null;
                    }
                });
    }

    @Override
    public <T> T find(final Class<T> entityClass)
    {
        T entity = flushContext.getConsistencyContext().executeWithReadConsistencyLevel(
                new SafeExecutionContext<T>()
                {
                    @Override
                    public T execute()
                    {
                        return loader.<T> load(ThriftPersistenceContext.this, entityClass);
                    }
                });

        if (entity != null)
        {
            entity = proxifier.buildProxy(entity, this);
        }
        return entity;
    }

    @Override
    public <T> T getReference(Class<T> entityClass)
    {
        setLoadEagerFields(false);
        return find(entityClass);
    }

    @Override
    public void refresh() throws AchillesStaleObjectStateException
    {
        flushContext.getConsistencyContext().executeWithReadConsistencyLevel(
                new SafeExecutionContext<Void>()
                {
                    @Override
                    public Void execute()
                    {
                        try {
                            refresher.refresh(ThriftPersistenceContext.this);
                        } catch (AchillesStaleObjectStateException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    }
                });

    }

    @Override
    public <T> T initialize(final T entity)
    {
        log.debug("Force lazy fields initialization for entity {}", entity);
        proxifier.ensureProxy(entity);
        final EntityInterceptor<ThriftPersistenceContext, T> interceptor = proxifier
                .getInterceptor(entity);

        flushContext.getConsistencyContext().executeWithReadConsistencyLevel(
                new SafeExecutionContext<Void>()
                {
                    @Override
                    public Void execute()
                    {
                        initializer.initializeEntity(entity, entityMeta, interceptor);
                        return null;
                    }
                });

        return entity;
    }

    public <T> T executeWithReadConsistencyLevel(SafeExecutionContext<T> context,
            ConsistencyLevel readLevel)
    {
        return flushContext.getConsistencyContext().executeWithReadConsistencyLevel(context,
                readLevel);
    }

    public <T> T executeWithWriteConsistencyLevel(SafeExecutionContext<T> context,
            ConsistencyLevel writeLevel)
    {
        return flushContext.getConsistencyContext().executeWithWriteConsistencyLevel(context,
                writeLevel);
    }

    public boolean isValueless()
    {
        return entityMeta.isValueless();
    }

    public ThriftGenericEntityDao findEntityDao(String tableName)
    {
        return daoContext.findEntityDao(tableName);
    }

    public ThriftGenericWideRowDao findWideRowDao(String tableName)
    {
        return daoContext.findWideRowDao(tableName);
    }

    public ThriftCounterDao getCounterDao()
    {
        return daoContext.getCounterDao();
    }

    public Mutator<Object> getEntityMutator(String tableName)
    {
        return flushContext.getEntityMutator(tableName);
    }

    public Mutator<Object> getWideRowMutator(String tableName)
    {
        return flushContext.getWideRowMutator(tableName);
    }

    public Mutator<Object> getCounterMutator()
    {
        return flushContext.getCounterMutator();
    }

    public ThriftGenericEntityDao getEntityDao()
    {
        return entityDao;
    }

    public ThriftGenericWideRowDao getWideRowDao()
    {
        return wideRowDao;
    }

}
