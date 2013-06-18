package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.lazyNonProxyType;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.util.AlreadyLoadedTransformer;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.EntityInterceptor;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;

/**
 * EntityInitializer
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityInitializer
{
	private static final Logger log = LoggerFactory.getLogger(EntityInitializer.class);

	public <T, CONTEXT extends PersistenceContext>
			void initializeEntity(T entity, EntityMeta entityMeta,
					EntityInterceptor<CONTEXT, T> interceptor)
	{

		log.debug("Initializing lazy fields for entity {} of class {}", entity,
				entityMeta.getClassName());

		Set<PropertyMeta<?, ?>> alreadyLoadedMetas = FluentIterable
				.from(interceptor.getAlreadyLoaded())
				.transform(new AlreadyLoadedTransformer(entityMeta.getGetterMetas()))
				.toImmutableSet();

		Set<PropertyMeta<?, ?>> allLazyMetas = FluentIterable
				.from(entityMeta.getPropertyMetas().values())
				.filter(lazyNonProxyType)
				.toImmutableSet();

		Set<PropertyMeta<?, ?>> toBeLoadedMetas = Sets.difference(allLazyMetas, alreadyLoadedMetas);

		for (PropertyMeta<?, ?> propertyMeta : toBeLoadedMetas)
		{
			try
			{
				propertyMeta.getGetter().invoke(entity);
			}
			catch (Throwable e)
			{
				log.error("Cannot initialize property '" + propertyMeta.getPropertyName()
						+ "' for entity '"
						+ entity + "'", e);
				throw new AchillesException(e);
			}
		}
	}
}
