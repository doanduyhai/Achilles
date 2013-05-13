package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityRefresher
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityRefresher
{
	private static final Logger log = LoggerFactory.getLogger(EntityRefresher.class);

	private EntityProxifier proxifier = new EntityProxifier();
	private EntityLoader loader = new EntityLoader();

	@SuppressWarnings("unchecked")
	public <ID, T> void refresh(AchillesPersistenceContext<ID> context)
	{
		log.debug("Refreshing entity of class {} and primary key {}", context.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());

		Object entity = context.getEntity();

		JpaEntityInterceptor<ID, T> interceptor = (JpaEntityInterceptor<ID, T>) proxifier
				.getInterceptor(entity);

		T freshEntity = (T) loader.load(context);

		interceptor.getDirtyMap().clear();
		interceptor.getLazyAlreadyLoaded().clear();
		interceptor.setTarget(freshEntity);
	}
}
