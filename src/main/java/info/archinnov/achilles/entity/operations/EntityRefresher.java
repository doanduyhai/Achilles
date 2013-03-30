package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

/**
 * EntityRefresher
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityRefresher
{

	private EntityProxifier proxifier = new EntityProxifier();
	private EntityLoader loader = new EntityLoader();

	@SuppressWarnings("unchecked")
	public <ID, T> void refresh(PersistenceContext<ID> context)
	{

		Object entity = context.getEntity();

		JpaEntityInterceptor<ID, T> interceptor = (JpaEntityInterceptor<ID, T>) proxifier
				.getInterceptor(entity);

		T freshEntity = this.loader.load(context);

		interceptor.getDirtyMap().clear();
		interceptor.getLazyAlreadyLoaded().clear();
		interceptor.setTarget(freshEntity);
	}
}
