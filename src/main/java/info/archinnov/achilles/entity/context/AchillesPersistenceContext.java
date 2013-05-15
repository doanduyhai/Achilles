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
public abstract class AchillesPersistenceContext<ID>
{
	protected final AchillesEntityIntrospector introspector = new AchillesEntityIntrospector();
	protected final EntityMeta<ID> entityMeta;
	protected final AchillesConfigurationContext configContext;
	protected final AchillesConsistencyLevelPolicy policy;
	protected final Class<?> entityClass;

	protected Object entity;
	protected ID primaryKey;
	protected AchillesFlushContext flushContext;

	protected AchillesPersistenceContext(EntityMeta<ID> entityMeta,
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

	protected AchillesPersistenceContext(EntityMeta<ID> entityMeta,
			AchillesConfigurationContext configContext, Class<?> entityClass, ID primaryKey,
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

	public abstract <JOIN_ID> AchillesPersistenceContext<JOIN_ID> newPersistenceContext(
			EntityMeta<JOIN_ID> joinMeta, Object joinEntity);

	public abstract <JOIN_ID> AchillesPersistenceContext<JOIN_ID> newPersistenceContext(
			Class<?> entityClass, EntityMeta<JOIN_ID> joinMeta, JOIN_ID joinId);

	public boolean isWideRow()
	{
		return this.entityMeta.isWideRow();
	}

	public String getColumnFamilyName()
	{
		return entityMeta.getColumnFamilyName();
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

	public EntityMeta<ID> getEntityMeta()
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

	public ID getPrimaryKey()
	{
		return primaryKey;
	}

	public void setPrimaryKey(ID primaryKey)
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

}
