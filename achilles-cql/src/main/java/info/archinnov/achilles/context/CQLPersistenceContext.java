package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;

/**
 * CQLPersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPersistenceContext extends AchillesPersistenceContext
{

	public CQLPersistenceContext(EntityMeta entityMeta, AchillesConfigurationContext configContext,
			Class<?> entityClass, Object primaryKey, AchillesFlushContext flushContext)
	{
		super(entityMeta, configContext, entityClass, primaryKey, flushContext);
	}

	@Override
	public AchillesPersistenceContext newPersistenceContext(EntityMeta joinMeta, Object joinEntity)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AchillesPersistenceContext newPersistenceContext(Class<?> entityClass,
			EntityMeta joinMeta, Object joinId)
	{
		// TODO Auto-generated method stub
		return null;
	}

}
