package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.io.Serializable;
import java.util.Map;

import net.sf.cglib.proxy.Factory;

/**
 * EntityRefresher
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityRefresher
{

	EntityHelper helper = new EntityHelper();
	EntityValidator entityValidator = new EntityValidator();
	EntityLoader loader = new EntityLoader();

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	public void refresh(Object entity, Map<Class<?>, EntityMeta<?>> entityMetaMap)
	{
		entityValidator.validateEntity(entity, entityMetaMap);

		Factory proxy = (Factory) entity;
		JpaEntityInterceptor interceptor = (JpaEntityInterceptor) proxy.getCallback(0);

		Class<?> entityClass = interceptor.getTarget().getClass();
		EntityMeta<?> entityMeta = entityMetaMap.get(entityClass);
		Object primaryKey = helper.determinePrimaryKey(entity, entityMeta);

		Object freshEntity = this.loader.load(entityClass, (Serializable) primaryKey,
				(EntityMeta) entityMeta);

		interceptor.getDirtyMap().clear();
		interceptor.getLazyLoaded().clear();
		interceptor.setTarget(freshEntity);
	}
}
