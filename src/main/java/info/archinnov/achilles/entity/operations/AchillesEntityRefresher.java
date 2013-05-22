package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AchillesEntityRefresher
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

	public <T> void refresh(AchillesPersistenceContext context)
	{
		log.debug("Refreshing entity of class {} and primary key {}", context
				.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());

		Object entity = context.getEntity();

		AchillesEntityInterceptor<Object> interceptor = proxifier.getInterceptor(entity);

		Object freshEntity = loader.load(context, context.getEntityClass());

		interceptor.getDirtyMap().clear();
		interceptor.getAlreadyLoaded().clear();
		interceptor.setTarget(freshEntity);
	}
}
