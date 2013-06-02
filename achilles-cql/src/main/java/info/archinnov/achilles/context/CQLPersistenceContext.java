package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.validation.Validator;

/**
 * CQLPersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPersistenceContext extends AchillesPersistenceContext
{

	private CQLDaoContext daoContext;

	private CQLAbstractFlushContext flushContext;

	public CQLPersistenceContext(EntityMeta entityMeta, AchillesConfigurationContext configContext,
			CQLDaoContext daoContext, AchillesFlushContext flushContext, Class<?> entityClass,
			Object primaryKey)
	{
		super(entityMeta, configContext, entityClass, primaryKey, flushContext);
		this.daoContext = daoContext;
		this.flushContext = (CQLAbstractFlushContext) flushContext;
	}

	public CQLPersistenceContext(EntityMeta entityMeta, AchillesConfigurationContext configContext,
			CQLDaoContext daoContext, AchillesFlushContext flushContext, Object entity)
	{
		super(entityMeta, configContext, entity, flushContext);
		this.daoContext = daoContext;
		this.flushContext = (CQLAbstractFlushContext) flushContext;
	}

	@Override
	public AchillesPersistenceContext newPersistenceContext(EntityMeta joinMeta, Object joinEntity)
	{
		Validator.validateNotNull(joinEntity, "join entity should not be null");
		return new CQLPersistenceContext(joinMeta, configContext, daoContext, flushContext,
				joinEntity);
	}

	@Override
	public AchillesPersistenceContext newPersistenceContext(Class<?> entityClass,
			EntityMeta joinMeta, Object joinId)
	{
		Validator.validateNotNull(entityClass, "entityClass should not be null");
		Validator.validateNotNull(joinId, "joinId should not be null");
		return new CQLPersistenceContext(joinMeta, configContext, daoContext, flushContext,
				entityClass, joinId);
	}

	public CQLDaoContext getDaoContext()
	{
		return daoContext;
	}

	public CQLAbstractFlushContext getFlushContext()
	{
		return flushContext;
	}

}
