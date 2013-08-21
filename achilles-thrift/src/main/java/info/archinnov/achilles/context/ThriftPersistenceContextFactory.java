package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.validation.Validator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                flushContext.duplicate(), joinEntity, OptionsBuilder.noOptions(), entitiesIdentity);
    }

    public ThriftPersistenceContext newContextForJoin(Class<?> entityClass, Object joinId,
            ThriftAbstractFlushContext<?> flushContext, Set<String> entitiesIdentity)
    {
        Validator.validateNotNull(entityClass, "entityClass should not be null for persistence context creation");
        Validator.validateNotNull(joinId, "joinId should not be null for persistence context creation");
        EntityMeta joinMeta = entityMetaMap.get(entityClass);
        return new ThriftPersistenceContext(joinMeta, configContext, daoContext,
                flushContext.duplicate(), entityClass, joinId, OptionsBuilder.noOptions(), entitiesIdentity);
    }

    //    public ThriftPersistenceContext newContextForBatch(Object entity, ThriftAbstractFlushContext<?> flushContext)
    //    {
    //        Validator.validateNotNull(entity, "entity should not be null for persistence context creation");
    //        Class<?> entityClass = proxifier.deriveBaseClass(entity);
    //        EntityMeta meta = entityMetaMap.get(entityClass);
    //
    //        return new ThriftPersistenceContext(meta, configContext, daoContext,
    //                flushContext, entity, new HashSet<String>());
    //    }

    //    public ThriftPersistenceContext newContextForBatch(Class<?> entityClass,
    //            Object primaryKey, ThriftAbstractFlushContext<?> flushContext)
    //    {
    //        log.trace("Initializing new persistence context for entity class {} and primary key {}",
    //                entityClass.getCanonicalName(), primaryKey);
    //
    //        Validator.validateNotNull(entityClass, "entityClass should not be null for persistence context creation");
    //        Validator.validateNotNull(primaryKey, "primaryKey should not be null for persistence context creation");
    //        EntityMeta meta = entityMetaMap.get(entityClass);
    //        return new ThriftPersistenceContext(meta, configContext, daoContext,
    //                flushContext, entityClass, primaryKey, OptionsBuilder.noOptions(), new HashSet<String>());
    //    }

    @Override
    public ThriftPersistenceContext newContext(Object entity, Options options)
    {
        log.trace("Initializing new persistence context for entity {}", entity);
        Validator.validateNotNull(entity, "entity should not be null for persistence context creation");
        Class<?> entityClass = proxifier.deriveBaseClass(entity);
        EntityMeta meta = entityMetaMap.get(entityClass);
        ThriftImmediateFlushContext flushContext = buildImmediateFlushContext(options);

        return new ThriftPersistenceContext(meta, configContext, daoContext,
                flushContext, entity, options, new HashSet<String>());
    }

    @Override
    public ThriftPersistenceContext newContext(Object entity)
    {
        return newContext(entity, OptionsBuilder.noOptions());
    }

    @Override
    public ThriftPersistenceContext newContext(Class<?> entityClass, Object primaryKey, Options options)
    {
        Validator.validateNotNull(entityClass, "entityClass should not be null for persistence context creation");
        Validator.validateNotNull(primaryKey, "primaryKey should not be null for persistence context creation");
        EntityMeta meta = entityMetaMap.get(entityClass);
        ThriftImmediateFlushContext flushContext = buildImmediateFlushContext(options);

        return new ThriftPersistenceContext(meta, configContext, daoContext,
                flushContext, entityClass, primaryKey, options, new HashSet<String>());
    }

    @Override
    public ThriftPersistenceContext newContextForSliceQuery(Class<?> entityClass, Object partitionKey,
            ConsistencyLevel cl)
    {
        EntityMeta meta = entityMetaMap.get(entityClass);
        PropertyMeta idMeta = meta.getIdMeta();
        Object embeddedId = invoker.instanciateEmbeddedIdWithPartitionKey(idMeta, partitionKey);

        ThriftImmediateFlushContext flushContext = buildImmediateFlushContext(OptionsBuilder.withConsistency(cl));

        return new ThriftPersistenceContext(meta, configContext, daoContext, flushContext, entityClass,
                embeddedId, OptionsBuilder.withConsistency(cl), new HashSet<String>());
    }

    private ThriftImmediateFlushContext buildImmediateFlushContext(Options options)
    {
        return new ThriftImmediateFlushContext(daoContext, configContext.getConsistencyPolicy(), options
                .getConsistencyLevel().orNull());
    }

}
