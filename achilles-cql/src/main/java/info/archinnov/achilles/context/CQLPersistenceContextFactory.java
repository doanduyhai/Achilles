package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import com.google.common.base.Optional;

/**
 * CQLPersistenceContextFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPersistenceContextFactory implements PersistenceContextFactory {

    public static final Optional<ConsistencyLevel> NO_CONSISTENCY_LEVEL = Optional
            .<ConsistencyLevel> absent();
    public static final Optional<Integer> NO_TTL = Optional.<Integer> absent();

    private CQLDaoContext daoContext;
    private ConfigurationContext configContext;
    private Map<Class<?>, EntityMeta> entityMetaMap;
    private CQLEntityProxifier proxifier = new CQLEntityProxifier();
    private ReflectionInvoker invoker = new ReflectionInvoker();

    public CQLPersistenceContextFactory(CQLDaoContext daoContext, ConfigurationContext configContext,
            Map<Class<?>, EntityMeta> entityMetaMap) {
        this.daoContext = daoContext;
        this.configContext = configContext;
        this.entityMetaMap = entityMetaMap;
    }

    public CQLPersistenceContext newContextForJoin(Object joinEntity, CQLAbstractFlushContext<?> flushContext,
            Set<String> entitiesIdentity)
    {
        Validator.validateNotNull(joinEntity, "join entity should not be null for persistence context creation");
        Class<?> entityClass = proxifier.deriveBaseClass(joinEntity);
        EntityMeta joinMeta = entityMetaMap.get(entityClass);

        return new CQLPersistenceContext(joinMeta, configContext, daoContext,
                flushContext.duplicateWithoutTtl(), joinEntity, entitiesIdentity);
    }

    public CQLPersistenceContext newContextForJoin(Class<?> entityClass,
            Object joinId, CQLAbstractFlushContext<?> flushContext,
            Set<String> entitiesIdentity)
    {
        Validator.validateNotNull(entityClass, "entityClass should not be null for persistence context creation");
        Validator.validateNotNull(joinId, "joinId should not be null for persistence context creation");
        EntityMeta joinMeta = entityMetaMap.get(entityClass);
        return new CQLPersistenceContext(joinMeta, configContext, daoContext,
                flushContext.duplicateWithoutTtl(), entityClass, joinId, entitiesIdentity);
    }

    @Override
    public CQLPersistenceContext newContext(Object entity, Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO)
    {
        Validator.validateNotNull(entity, "entity should not be null for persistence context creation");
        Class<?> entityClass = proxifier.deriveBaseClass(entity);
        EntityMeta meta = entityMetaMap.get(entityClass);
        CQLImmediateFlushContext flushContext = buildImmediateFlushContext(readLevelO, writeLevelO, ttlO);

        return new CQLPersistenceContext(meta, configContext, daoContext,
                flushContext, entity, new HashSet<String>());
    }

    @Override
    public CQLPersistenceContext newContext(Object entity)
    {
        return newContext(entity, NO_CONSISTENCY_LEVEL, NO_CONSISTENCY_LEVEL, NO_TTL);
    }

    @Override
    public CQLPersistenceContext newContext(Class<?> entityClass, Object primaryKey,
            Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO)
    {
        Validator.validateNotNull(entityClass, "entityClass should not be null for persistence context creation");
        Validator.validateNotNull(primaryKey, "primaryKey should not be null for persistence context creation");
        EntityMeta meta = entityMetaMap.get(entityClass);
        CQLImmediateFlushContext flushContext = buildImmediateFlushContext(readLevelO, writeLevelO, ttlO);

        return new CQLPersistenceContext(meta, configContext, daoContext,
                flushContext, entityClass, primaryKey, new HashSet<String>());
    }

    @Override
    public CQLPersistenceContext newContextForSliceQuery(Class<?> entityClass, Object partitionKey,
            ConsistencyLevel cl)
    {
        EntityMeta meta = entityMetaMap.get(entityClass);
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Object embeddedId = invoker.instanciateEmbeddedIdWithPartitionKey(idMeta, partitionKey);

        CQLImmediateFlushContext flushContext = buildImmediateFlushContext(Optional.fromNullable(cl),
                Optional.fromNullable(cl), NO_TTL);

        return new CQLPersistenceContext(meta, configContext, daoContext, flushContext, entityClass,
                embeddedId, new HashSet<String>());
    }

    private CQLImmediateFlushContext buildImmediateFlushContext(Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO)
    {
        return new CQLImmediateFlushContext(daoContext, readLevelO, writeLevelO, ttlO);
    }
}
