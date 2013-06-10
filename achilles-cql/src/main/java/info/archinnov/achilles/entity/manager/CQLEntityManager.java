package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLImmediateFlushContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.EntityValidator;

import java.util.Map;

/**
 * CqlEntityManager
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityManager extends AchillesEntityManager<CQLPersistenceContext>
{
	private CQLDaoContext daoContext;

	protected CQLEntityManager(AchillesEntityManagerFactory entityManagerFactory,
			Map<Class<?>, EntityMeta> entityMetaMap, //
			ConfigurationContext configContext, CQLDaoContext daoContext)
	{
		super(entityManagerFactory, entityMetaMap, configContext);
		this.daoContext = daoContext;
		super.proxifier = new CQLEntityProxifier();
		super.entityValidator = new EntityValidator<CQLPersistenceContext>(proxifier);
	}

	@Override
	protected CQLPersistenceContext initPersistenceContext(Object entity)
	{
		EntityMeta entityMeta = this.entityMetaMap.get(proxifier.deriveBaseClass(entity));
		return new CQLPersistenceContext(entityMeta, configContext, daoContext,
				new CQLImmediateFlushContext(daoContext), entity);
	}

	@Override
	protected CQLPersistenceContext initPersistenceContext(Class<?> entityClass, Object primaryKey)
	{
		EntityMeta entityMeta = this.entityMetaMap.get(entityClass);
		return new CQLPersistenceContext(entityMeta, configContext, daoContext,
				new CQLImmediateFlushContext(daoContext), entityClass, primaryKey);
	}

}
