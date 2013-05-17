package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.interceptor.AchillesJpaEntityInterceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityRefresher
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesEntityRefresher
{
	private static final Logger log = LoggerFactory.getLogger(AchillesEntityRefresher.class);

	private AchillesEntityProxifier proxifier;
	private AchillesEntityLoader loader;

	public AchillesEntityRefresher() {}

	public AchillesEntityRefresher(AchillesEntityLoader loader, AchillesEntityProxifier proxifier) {
		this.loader = loader;
		this.proxifier = proxifier;
	}

	@SuppressWarnings("unchecked")
	public <ID, T> void refresh(AchillesPersistenceContext<ID> context)
	{
		log.debug("Refreshing entity of class {} and primary key {}", context.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());

		Object entity = context.getEntity();

		AchillesJpaEntityInterceptor<ID, T> interceptor = (AchillesJpaEntityInterceptor<ID, T>) proxifier
				.getInterceptor(entity);

		T freshEntity = (T) loader.load(context);

		interceptor.getDirtyMap().clear();
		interceptor.getLazyAlreadyLoaded().clear();
		interceptor.setTarget(freshEntity);
	}
}
