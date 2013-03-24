package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

/**
 * EntityRefresher
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityRefresher
{

	private EntityHelper helper = new EntityHelper();
	private EntityLoader loader = new EntityLoader();

	@SuppressWarnings("unchecked")
	public <ID, T> void refresh(PersistenceContext<ID> context)
	{

		Object entity = context.getEntity();

		JpaEntityInterceptor<ID, T> interceptor = (JpaEntityInterceptor<ID, T>) helper
				.getInterceptor(entity);

		T freshEntity = this.loader.load(context);

		interceptor.getDirtyMap().clear();
		interceptor.getLazyAlreadyLoaded().clear();
		interceptor.setTarget(freshEntity);
	}
}
