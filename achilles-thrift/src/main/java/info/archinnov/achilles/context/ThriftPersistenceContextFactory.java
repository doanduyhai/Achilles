package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;

/**
 * ThriftPersistenceContextFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPersistenceContextFactory implements PersistenceContextFactory {

    private static final Logger log = LoggerFactory.getLogger(ThriftPersistenceContextFactory.class);

    private ThriftDaoContext daoContext;
    private ConfigurationContext configContext;
    private Map<Class<?>, EntityMeta> entityMetaMap;

    private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
    private ReflectionInvoker invoker = new ReflectionInvoker();

    public ThriftPersistenceContextFactory(ThriftDaoContext daoContext, ConfigurationContext configContext,
            Map<Class<?>, EntityMeta> entityMetaMap) {
        this.daoContext = daoContext;
        this.configContext = configContext;
        this.entityMetaMap = entityMetaMap;
    }

    public ThriftPersistenceContext newContextForJoin(Object joinEntity, ThriftAbstractFlushContext<?> flushContext,
            Set<String> entitiesIdentity)
    {
        Validator.validateNotNull(joinEntity, "join entity should not be null for persistence context creation");
        Class<?> entityClass = proxifier.deriveBaseClass(joinEntity);
        EntityMeta joinMeta = entityMetaMap.get(entityClass);

        return new ThriftPersistenceContext(joinMeta, configContext, daoContext,
                flushContext.duplicateWithoutTtl(), joinEntity, entitiesIdentity);
    }

    public ThriftPersistenceContext newContextForJoin(Class<?> entityClass, Object joinId,
            ThriftAbstractFlushContext<?> flushContext, Set<String> entitiesIdentity)
    {
        Validator.validateNotNull(entityClass, "entityClass should not be null for persistence context creation");
        Validator.validateNotNull(joinId, "joinId should not be null for persistence context creation");
        EntityMeta joinMeta = entityMetaMap.get(entityClass);
        return new ThriftPersistenceContext(joinMeta, configContext, daoContext,
                flushContext.duplicateWithoutTtl(), entityClass, joinId, entitiesIdentity);
    }

    public ThriftPersistenceContext newContextForBatch(Object entity, ThriftAbstractFlushContext<?> flushContext)
    {
        Validator.validateNotNull(entity, "entity should not be null for persistence context creation");
        Class<?> entityClass = proxifier.deriveBaseClass(entity);
        EntityMeta meta = entityMetaMap.get(entityClass);

        return new ThriftPersistenceContext(meta, configContext, daoContext,
                flushContext, entity, new HashSet<String>());
    }

    public ThriftPersistenceContext newContextForBatch(Class<?> entityClass,
            Object primaryKey, ThriftAbstractFlushContext<?> flushContext)
    {
        log.trace("Initializing new persistence context for entity class {} and primary key {}",
                entityClass.getCanonicalName(), primaryKey);

        Validator.validateNotNull(entityClass, "entityClass should not be null for persistence context creation");
        Validator.validateNotNull(primaryKey, "primaryKey should not be null for persistence context creation");
        EntityMeta meta = entityMetaMap.get(entityClass);
        return new ThriftPersistenceContext(meta, configContext, daoContext,
                flushContext, entityClass, primaryKey, new HashSet<String>());
    }

    @Override
    public ThriftPersistenceContext newContext(Object entity, Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
    {
        log.trace("Initializing new persistence context for entity {}", entity);
        Validator.validateNotNull(entity, "entity should not be null for persistence context creation");
        Class<?> entityClass = proxifier.deriveBaseClass(entity);
        EntityMeta meta = entityMetaMap.get(entityClass);
        ThriftImmediateFlushContext flushContext = buildImmediateFlushContext(readLevelO, writeLevelO, ttlO);

        return new ThriftPersistenceContext(meta, configContext, daoContext,
                flushContext, entity, new HashSet<String>());
    }

    @Override
    public ThriftPersistenceContext newContext(Object entity)
    {
        return newContext(entity, NO_CONSISTENCY_LEVEL, NO_CONSISTENCY_LEVEL, NO_TTL);
    }

    @Override
    public ThriftPersistenceContext newContext(Class<?> entityClass, Object primaryKey,
            Optional<ConsistencyLevel> readLevelO, Optional<ConsistencyLevel> writeLevelO, Optional<Integer> ttlO)
    {
        Validator.validateNotNull(entityClass, "entityClass should not be null for persistence context creation");
        Validator.validateNotNull(primaryKey, "primaryKey should not be null for persistence context creation");
        EntityMeta meta = entityMetaMap.get(entityClass);
        ThriftImmediateFlushContext flushContext = buildImmediateFlushContext(readLevelO, writeLevelO, ttlO);

        return new ThriftPersistenceContext(meta, configContext, daoContext,
                flushContext, entityClass, primaryKey, new HashSet<String>());
    }

    @Override
    public ThriftPersistenceContext newContextForSliceQuery(Class<?> entityClass, Object partitionKey,
            ConsistencyLevel cl)
    {
        EntityMeta meta = entityMetaMap.get(entityClass);
        PropertyMeta idMeta = meta.getIdMeta();
        Object embeddedId = invoker.instanciateEmbeddedIdWithPartitionKey(idMeta, partitionKey);

        ThriftImmediateFlushContext flushContext = buildImmediateFlushContext(Optional.fromNullable(cl),
                Optional.fromNullable(cl), NO_TTL);

        return new ThriftPersistenceContext(meta, configContext, daoContext, flushContext, entityClass,
                embeddedId, new HashSet<String>());
    }

    private ThriftImmediateFlushContext buildImmediateFlushContext(Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO)
    {
        return new ThriftImmediateFlushContext(daoContext, configContext.getConsistencyPolicy(), readLevelO,
                writeLevelO, ttlO);
    }

}
