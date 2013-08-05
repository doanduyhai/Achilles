package info.archinnov.achilles.context;

import info.archinnov.achilles.context.FlushContext.FlushType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.ObjectUtils;
import com.google.common.base.Optional;

/**
 * PersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class PersistenceContext
{
    protected ReflectionInvoker invoker = new ReflectionInvoker();
    protected EntityInitializer initializer = new EntityInitializer();
    protected ConfigurationContext configContext;
    protected Class<?> entityClass;
    protected EntityMeta entityMeta;
    protected Object entity;
    protected Object primaryKey;
    protected Object partitionKey;
    protected FlushContext<?> flushContext;
    protected Set<String> entitiesIdentity;

    protected boolean loadEagerFields = true;

    private PersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext,
            FlushContext<?> flushContext, Class<?> entityClass, Set<String> entitiesIdentity)
    {
        this.entityMeta = entityMeta;
        this.configContext = configContext;
        this.flushContext = flushContext;
        this.entityClass = entityClass;
        this.entitiesIdentity = entitiesIdentity;

    }

    protected PersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext,
            Object entity, FlushContext<?> flushContext, Set<String> entitiesIdentity)
    {
        this(entityMeta, configContext, flushContext, entityMeta.getEntityClass(), entitiesIdentity);
        Validator.validateNotNull(entity, "The entity should not be null for persistence context creation");
        this.entity = entity;
        this.primaryKey = invoker.getPrimaryKey(entity, entityMeta.getIdMeta());
        Validator.validateNotNull(primaryKey,
                "The primary key for the entity '%s' should not be null for persistence context creation", entity);

    }

    protected PersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext,
            Class<?> entityClass, Object primaryKey, FlushContext<?> flushContext, Set<String> entitiesIdentity)
    {
        this(entityMeta, configContext, flushContext, entityClass, entitiesIdentity);

        this.primaryKey = primaryKey;
        this.flushContext = flushContext;
        Validator.validateNotNull(primaryKey,
                "The primary key for the entity '%s' should not be null for persistence context creation", entity);

    }

    private void extractPartitionKey()
    {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        if (idMeta.isEmbeddedId())
        {
            this.partitionKey = invoker.getPartitionKey(primaryKey, idMeta);
        }
    }

    public boolean addToProcessingList(Object entity)
    {
        return entitiesIdentity.add(ObjectUtils.identityToString(entity));
    }

    public abstract void persist();

    public abstract <T> T merge(T entity);

    public abstract void remove();

    public abstract <T> T find(Class<T> entityClass);

    public abstract <T> T getReference(Class<T> entityClass);

    public abstract <T> T initialize(T entity);

    public <T> List<T> initialize(List<T> entities)
    {
        for (T entity : entities)
        {
            initialize(entity);
        }
        return entities;
    }

    public <T> Set<T> initialize(Set<T> entities)
    {
        for (T entity : entities)
        {
            initialize(entity);
        }
        return entities;
    }

    public PropertyMeta<?, ?> getIdMeta()
    {
        return entityMeta.getIdMeta();
    }

    public PropertyMeta<?, ?> getFirstMeta()
    {
        return entityMeta.getAllMetasExceptIdMeta().get(0);
    }

    public abstract void refresh() throws AchillesStaleObjectStateException;

    public abstract PersistenceContext duplicate(Object entity);

    public abstract PersistenceContext createContextForJoin(EntityMeta joinMeta, Object joinEntity);

    public abstract PersistenceContext createContextForJoin(Class<?> entityClass,
            EntityMeta joinMeta, Object joinId);

    public boolean isClusteredEntity()
    {
        return this.entityMeta.isClusteredEntity();
    }

    public String getTableName()
    {
        return entityMeta.getTableName();
    }

    public boolean isBatchMode()
    {
        return flushContext.type() == FlushType.BATCH;
    }

    public void flush()
    {
        flushContext.flush();
    }

    public void endBatch()
    {
        flushContext.endBatch();
    }

    public void setReadConsistencyLevelO(Optional<ConsistencyLevel> readLevelO)
    {
        flushContext.setReadConsistencyLevel(readLevelO);
    }

    public void setWriteConsistencyLevelO(Optional<ConsistencyLevel> writeLevelO)
    {
        flushContext.setWriteConsistencyLevel(writeLevelO);
    }

    public void reinitConsistencyLevels()
    {
        flushContext.reinitConsistencyLevels();
    }

    public Optional<Integer> getTttO()
    {
        return flushContext.getTtlO();
    }

    public EntityMeta getEntityMeta()
    {
        return entityMeta;
    }

    public Object getEntity()
    {
        return entity;
    }

    public void setEntity(Object entity)
    {
        this.entity = entity;
    }

    public Class<?> getEntityClass()
    {
        return entityClass;
    }

    public Object getPrimaryKey()
    {
        return primaryKey;
    }

    public void setPrimaryKey(Object primaryKey)
    {
        this.primaryKey = primaryKey;
    }

    public Object getPartitionKey()
    {
        if (partitionKey == null && primaryKey != null)
        {
            extractPartitionKey();
        }
        return partitionKey;
    }

    public void setPartitionKey(Object partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    public ConfigurationContext getConfigContext()
    {
        return configContext;
    }

    public void setEntityMeta(EntityMeta entityMeta)
    {
        this.entityMeta = entityMeta;
    }

    public void setFlushContext(FlushContext<?> flushContext)
    {
        this.flushContext = flushContext;
    }

    public boolean isLoadEagerFields()
    {
        return loadEagerFields;
    }

    public void setLoadEagerFields(boolean loadEagerFields)
    {
        this.loadEagerFields = loadEagerFields;
    }

    public Optional<ConsistencyLevel> getReadConsistencyLevel()
    {
        return flushContext.getReadConsistencyLevel();
    }

    public Optional<ConsistencyLevel> getWriteConsistencyLevel()
    {
        return flushContext.getReadConsistencyLevel();
    }

}
