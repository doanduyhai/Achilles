package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.compound.ThriftCompoundKeyValidator;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftSliceQueryExecutor;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;

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
public class ThriftEntityManager extends EntityManager<ThriftPersistenceContext>
{
    private static final Logger log = LoggerFactory.getLogger(ThriftEntityManager.class);

    protected ThriftDaoContext thriftDaoContext;
    private ThriftSliceQueryExecutor queryExecutor;
    private ThriftCompoundKeyValidator compoundKeyValidator = new ThriftCompoundKeyValidator();

    ThriftEntityManager(EntityManagerFactory entityManagerFactory,
            Map<Class<?>, EntityMeta> entityMetaMap, //
            ThriftDaoContext thriftDaoContext, //
            ConfigurationContext configContext)
    {
        super(entityManagerFactory, entityMetaMap, configContext);
        this.thriftDaoContext = thriftDaoContext;
        super.proxifier = new ThriftEntityProxifier();
        super.entityValidator = new EntityValidator<ThriftPersistenceContext>(super.proxifier);
        this.queryExecutor = new ThriftSliceQueryExecutor(configContext, thriftDaoContext, consistencyPolicy);
    }

    /**
     * Create a new state-full EntityManager for batch handling <br/>
     * <br/>
     * 
     * <strong>WARNING : This EntityManager is state-full and not thread-safe. In case of exception, you MUST not
     * re-use it but create another one</strong>
     * 
     * @return a new state-full EntityManager
     */
    public ThriftBatchingEntityManager batchingEntityManager()
    {
        return new ThriftBatchingEntityManager(entityManagerFactory, entityMetaMap,
                thriftDaoContext, configContext);
    }

    /**
     * Create a new slice query builder for entity of type T<br/>
     * <br/>
     * 
     * @param entityClass
     *            Entity class
     * @return SliceQueryBuilder<T>
     */
    public <T> SliceQueryBuilder<ThriftPersistenceContext, T> sliceQuery(Class<T> entityClass)
    {
        EntityMeta meta = entityMetaMap.get(entityClass);
        return new SliceQueryBuilder<ThriftPersistenceContext, T>(queryExecutor, compoundKeyValidator, entityClass,
                meta);
    }

    @Override
    protected ThriftPersistenceContext initPersistenceContext(Class<?> entityClass,
            Object primaryKey,
            Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO)
    {
        log.trace("Initializing new persistence context for entity class {} and primary key {}",
                entityClass.getCanonicalName(), primaryKey);

        EntityMeta entityMeta = entityMetaMap.get(entityClass);

        ThriftImmediateFlushContext flushContext = new ThriftImmediateFlushContext(
                thriftDaoContext,
                consistencyPolicy, readLevelO, writeLevelO, ttlO);

        ThriftPersistenceContext context = new ThriftPersistenceContext(entityMeta, configContext,
                thriftDaoContext,
                flushContext, entityClass, primaryKey, new HashSet<String>());
        return context;
    }

    @Override
    protected ThriftPersistenceContext initPersistenceContext(Object entity,
            Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
    {
        log.trace("Initializing new persistence context for entity {}", entity);

        EntityMeta entityMeta = this.entityMetaMap.get(proxifier.deriveBaseClass(entity));
        ThriftImmediateFlushContext flushContext = new ThriftImmediateFlushContext(
                thriftDaoContext,
                consistencyPolicy, readLevelO, writeLevelO, ttlO);

        return new ThriftPersistenceContext(entityMeta, configContext, thriftDaoContext,
                flushContext, entity,
                new HashSet<String>());
    }

    protected void setThriftDaoContext(ThriftDaoContext thriftDaoContext) {
        this.thriftDaoContext = thriftDaoContext;
    }

    public void setQueryExecutor(ThriftSliceQueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    public void setCompoundKeyValidator(ThriftCompoundKeyValidator compoundKeyValidator) {
        this.compoundKeyValidator = compoundKeyValidator;
    }

}
