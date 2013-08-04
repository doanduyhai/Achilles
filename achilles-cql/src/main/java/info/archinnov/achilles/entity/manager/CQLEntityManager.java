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
import info.archinnov.achilles.query.cql.CQLNativeQueryBuilder;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.query.typed.CQLTypedQueryBuilder;
import info.archinnov.achilles.query.typed.CQLTypedQueryValidator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;
import java.util.Map;
import com.datastax.driver.core.Session;
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
    protected CQLDaoContext daoContext;

    private CQLTypedQueryValidator typedQueryValidator = new CQLTypedQueryValidator();

    protected CQLEntityManager(Map<Class<?>, EntityMeta> entityMetaMap, //
            CQLPersistenceContextFactory contextFactory,
            CQLDaoContext daoContext,
            ConfigurationContext configContext)
    {
        super(entityMetaMap, configContext);
        this.daoContext = daoContext;
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

    /**
     * Return a CQL native query builder
     * 
     * @param queryString
     *            native CQL query string, including limit, ttl and consistency options
     * 
     * @return CQLNativeQueryBuilder
     */
    public CQLNativeQueryBuilder nativeQuery(String queryString)
    {
        Validator.validateNotBlank(queryString, "The query string for native query should not be blank");
        return new CQLNativeQueryBuilder(daoContext, queryString);
    }

    /**
     * Return a CQL typed query builder
     * 
     * All found entities will be in 'managed' state
     * 
     * @param entityClass
     *            type of entity to be returned
     * 
     * @param queryString
     *            native CQL query string, including limit, ttl and consistency options
     * 
     * @return CQLTypedQueryBuilder<T>
     */
    public <T> CQLTypedQueryBuilder<T> typedQuery(Class<T> entityClass, String queryString)
    {
        Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
        Validator.validateNotBlank(queryString, "The query string for typed query should not be blank");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass),
                "Cannot perform typed query because the entityClass '" + entityClass.getCanonicalName()
                        + "' is not managed by Achilles");

        EntityMeta meta = entityMetaMap.get(entityClass);
        typedQueryValidator.validateTypedQuery(entityClass, queryString, meta);
        return new CQLTypedQueryBuilder<T>(entityClass, daoContext, queryString, meta, contextFactory, true);
    }

    /**
     * Return a CQL typed query builder
     * 
     * All found entities will be returned as raw entities and not 'managed' by Achilles
     * 
     * @param entityClass
     *            type of entity to be returned
     * 
     * @param queryString
     *            native CQL query string, including limit, ttl and consistency options
     * 
     * @return CQLTypedQueryBuilder<T>
     */
    public <T> CQLTypedQueryBuilder<T> rawTypedQuery(Class<T> entityClass, String queryString)
    {
        Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
        Validator.validateNotBlank(queryString, "The query string for typed query should not be blank");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass),
                "Cannot perform typed query because the entityClass '" + entityClass.getCanonicalName()
                        + "' is not managed by Achilles");

        EntityMeta meta = entityMetaMap.get(entityClass);
        typedQueryValidator.validateTypedQuery(entityClass, queryString, meta);
        return new CQLTypedQueryBuilder<T>(entityClass, daoContext, queryString, meta, contextFactory, false);
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

    public Session getNativeSession()
    {
        return daoContext.getSession();
    }

}
