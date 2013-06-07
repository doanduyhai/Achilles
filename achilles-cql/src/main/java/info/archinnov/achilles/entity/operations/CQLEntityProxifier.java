package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;
import info.archinnov.achilles.proxy.CQLEntityInterceptorBuilder;

/**
 * CQLEntityProxifier
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityProxifier extends EntityProxifier<CQLPersistenceContext> {

    @Override
    public <T> CQLEntityInterceptor<T> buildInterceptor(CQLPersistenceContext context, T entity) {
        return new CQLEntityInterceptorBuilder<T>(context, entity).build();
    }

}
