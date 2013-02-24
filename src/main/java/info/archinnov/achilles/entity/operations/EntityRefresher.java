package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.util.Map;

/**
 * EntityRefresher
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityRefresher
{

	private EntityHelper helper = new EntityHelper();
	private EntityValidator entityValidator = new EntityValidator();
	private EntityLoader loader = new EntityLoader();

	@SuppressWarnings("unchecked")
	public <ID, T> void refresh(T entity, Map<Class<?>, EntityMeta<?>> entityMetaMap)
	{
		entityValidator.validateEntity(entity, entityMetaMap);

		if (!helper.isProxy(entity))
		{
			throw new IllegalArgumentException("The entity '" + entity
					+ "' is not in 'managed' state. Please use em.merge() to merge it first");
		}

		JpaEntityInterceptor<ID, T> interceptor = helper.getInterceptor(entity);

		Class<T> entityClass = (Class<T>) interceptor.getTarget().getClass();
		EntityMeta<T> entityMeta = (EntityMeta<T>) entityMetaMap.get(entityClass);
		T primaryKey = (T) helper.determinePrimaryKey(entity, entityMeta);

		T freshEntity = this.loader.load(entityClass, primaryKey, entityMeta);

		interceptor.getDirtyMap().clear();
		interceptor.getLazyLoaded().clear();
		interceptor.setTarget(freshEntity);
	}
}
