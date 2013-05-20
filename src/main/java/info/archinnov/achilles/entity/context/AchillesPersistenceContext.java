package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.AchillesEntityIntrospector;
import info.archinnov.achilles.entity.context.AchillesFlushContext.FlushType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

/**
 * AchillesPersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AchillesPersistenceContext
{
	protected final AchillesEntityIntrospector introspector = new AchillesEntityIntrospector();
	protected final AchillesConfigurationContext configContext;
	protected final AchillesConsistencyLevelPolicy policy;
	protected final Class<?> entityClass;

	protected EntityMeta entityMeta;
	protected Object entity;
	protected Object primaryKey;
	protected AchillesFlushContext flushContext;

	protected AchillesPersistenceContext(EntityMeta entityMeta,
			AchillesConfigurationContext configContext, Object entity,
			AchillesFlushContext flushContext)
	{
		Validator.validateNotNull(entity, "The entity '" + entity + "' should not be null");

		this.entityMeta = entityMeta;
		this.configContext = configContext;
		this.entity = entity;
		this.flushContext = flushContext;
		this.policy = configContext.getConsistencyPolicy();
		this.entityClass = entity.getClass();
		this.primaryKey = introspector.getKey(entity, entityMeta.getIdMeta());

		Validator.validateNotNull(primaryKey, "The primary key for the entity '" + entity
				+ "' should not be null");
	}

	protected AchillesPersistenceContext(EntityMeta entityMeta,
			AchillesConfigurationContext configContext, Class<?> entityClass, Object primaryKey,
			AchillesFlushContext flushContext)
	{
		this.entityMeta = entityMeta;
		this.configContext = configContext;
		this.entityClass = entityClass;
		this.primaryKey = primaryKey;
		this.flushContext = flushContext;
		this.policy = configContext.getConsistencyPolicy();

		Validator.validateNotNull(primaryKey, "The primary key for the entity '" + entity
				+ "' should not be null");
	}

	public abstract AchillesPersistenceContext newPersistenceContext(EntityMeta joinMeta,
			Object joinEntity);

	public abstract AchillesPersistenceContext newPersistenceContext(Class<?> entityClass,
			EntityMeta joinMeta, Object joinId);

	public boolean isWideRow()
	{
		return this.entityMeta.isWideRow();
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

	public void setReadConsistencyLevel(ConsistencyLevel readLevel)
	{
		flushContext.setReadConsistencyLevel(readLevel);
	}

	public void setWriteConsistencyLevel(ConsistencyLevel writeLevel)
	{
		flushContext.setWriteConsistencyLevel(writeLevel);
	}

	public void reinitConsistencyLevels()
	{
		flushContext.reinitConsistencyLevels();
	}

	public void cleanUpFlushContext()
	{
		flushContext.cleanUp();
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

	public AchillesConfigurationContext getConfigContext()
	{
		return configContext;
	}

	public AchillesConsistencyLevelPolicy getPolicy()
	{
		return policy;
	}

	public void setEntityMeta(EntityMeta entityMeta)
	{
		this.entityMeta = entityMeta;
	}

	public void setFlushContext(AchillesFlushContext flushContext)
	{
		this.flushContext = flushContext;
	}

}
