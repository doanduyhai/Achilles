package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.compound.CQLCompoundKeyValidator;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.CQLPersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.CQLSliceQueryExecutor;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.Map;
import com.google.common.base.Optional;

/**
 * CqlEntityManager
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityManager extends EntityManager<CQLPersistenceContext>
{
    private CQLCompoundKeyValidator compoundKeyValidator = new CQLCompoundKeyValidator();
    private CQLSliceQueryExecutor sliceQueryExecutor;
    private CQLPersistenceContextFactory contextFactory;

    protected CQLEntityManager(Map<Class<?>, EntityMeta> entityMetaMap, //
            CQLPersistenceContextFactory contextFactory,
            CQLDaoContext daoContext,
            ConfigurationContext configContext)
    {
        super(entityMetaMap, configContext);
        super.proxifier = new CQLEntityProxifier();
        super.entityValidator = new EntityValidator<CQLPersistenceContext>(proxifier);
        this.contextFactory = contextFactory;
        this.sliceQueryExecutor = new CQLSliceQueryExecutor(contextFactory, configContext, daoContext);
    }

    @Override
    public <T> SliceQueryBuilder<CQLPersistenceContext, T> sliceQuery(Class<T> entityClass)
    {
        EntityMeta meta = entityMetaMap.get(entityClass);
        return new SliceQueryBuilder<CQLPersistenceContext, T>(sliceQueryExecutor, compoundKeyValidator, entityClass,
                meta);
    }

    @Override
    protected CQLPersistenceContext initPersistenceContext(Object entity, Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
    {
        return contextFactory.newContext(entity, readLevelO, writeLevelO, ttlO);
    }

    @Override
    protected CQLPersistenceContext initPersistenceContext(Class<?> entityClass, Object primaryKey,
            Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO)
    {
        return contextFactory.newContext(entityClass, primaryKey, readLevelO, writeLevelO, ttlO);
    }

}
