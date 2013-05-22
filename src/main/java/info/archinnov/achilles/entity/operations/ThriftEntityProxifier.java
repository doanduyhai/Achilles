package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;
import info.archinnov.achilles.proxy.ThriftEntityInterceptorBuilder;

/**
 * ThriftEntityProxifier
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityProxifier extends AchillesEntityProxifier
{

	@Override
	public <T> AchillesEntityInterceptor<T> buildInterceptor(AchillesPersistenceContext context,
			T entity)
	{
		return ThriftEntityInterceptorBuilder.builder(context, entity).build();
	}

}
