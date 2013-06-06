package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;

/**
 * CQLEntityProxifier
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityProxifier extends AchillesEntityProxifier<CQLPersistenceContext>
{

	@Override
	public <T> CQLEntityInterceptor<T> buildInterceptor(AchillesPersistenceContext context, T entity)
	{
		// TODO Auto-generated method stub
		return null;
	}

}
