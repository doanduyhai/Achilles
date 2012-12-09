package fr.doan.achilles.entity.operations;

import java.io.Serializable;
import java.util.Map;

import net.sf.cglib.proxy.Factory;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.proxy.interceptor.JpaInterceptor;

/**
 * EntityRefresher
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityRefresher
{

	private EntityWrapperUtil util = new EntityWrapperUtil();
	private EntityValidator entityValidator = new EntityValidator();
	private EntityLoader loader = new EntityLoader();

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	public void refresh(Object entity, Map<Class<?>, EntityMeta<?>> entityMetaMap)
	{
		entityValidator.validateEntity(entity, entityMetaMap);

		Factory proxy = (Factory) entity;
		JpaInterceptor interceptor = (JpaInterceptor) proxy.getCallback(0);

		Class<?> entityClass = interceptor.getTarget().getClass();
		EntityMeta<?> entityMeta = entityMetaMap.get(entityClass);
		Object primaryKey = util.determinePrimaryKey(entity, entityMeta);

		Object freshEntity = this.loader.load(entityClass, (Serializable) primaryKey,
				(EntityMeta) entityMeta);

		interceptor.getDirtyMap().clear();
		interceptor.getLazyLoaded().clear();
		interceptor.setTarget(freshEntity);
	}
}
