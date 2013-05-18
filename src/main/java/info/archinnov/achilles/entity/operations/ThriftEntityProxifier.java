package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.interceptor.AchillesJpaEntityInterceptor;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptorBuilder;

/**
 * ThriftEntityProxifier
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityProxifier extends AchillesEntityProxifier
{

	@Override
	public <T> AchillesJpaEntityInterceptor<T> buildInterceptor(AchillesPersistenceContext context,
			T entity)
	{
		return JpaEntityInterceptorBuilder.builder(context, entity).build();
	}

}
