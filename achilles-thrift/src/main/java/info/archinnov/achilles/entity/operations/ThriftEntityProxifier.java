package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.proxy.ThriftEntityInterceptorBuilder;

/**
 * ThriftEntityProxifier
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityProxifier extends EntityProxifier<ThriftPersistenceContext> {

    @Override
    public <T> ThriftEntityInterceptor<T> buildInterceptor(ThriftPersistenceContext context, T entity) {
        return ThriftEntityInterceptorBuilder.builder(context, entity).build();
    }

}
