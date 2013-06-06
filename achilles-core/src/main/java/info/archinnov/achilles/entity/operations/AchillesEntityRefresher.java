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
public class AchillesEntityRefresher<CONTEXT extends AchillesPersistenceContext>
{
	private static final Logger log = LoggerFactory.getLogger(AchillesEntityRefresher.class);

	private AchillesEntityProxifier<CONTEXT> proxifier;
	private AchillesEntityLoader<CONTEXT> loader;

	public AchillesEntityRefresher() {}

	public AchillesEntityRefresher(AchillesEntityLoader<CONTEXT> loader,
			AchillesEntityProxifier<CONTEXT> proxifier)
	{
		this.loader = loader;
		this.proxifier = proxifier;
	}

	public <T> void refresh(CONTEXT context)
	{
		log.debug("Refreshing entity of class {} and primary key {}", context
				.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());

		Object entity = context.getEntity();

		AchillesEntityInterceptor<CONTEXT, Object> interceptor = proxifier.getInterceptor(entity);

		Object freshEntity = loader.load(context, context.getEntityClass());

		interceptor.getDirtyMap().clear();
		interceptor.getAlreadyLoaded().clear();
		interceptor.setTarget(freshEntity);
	}
}
