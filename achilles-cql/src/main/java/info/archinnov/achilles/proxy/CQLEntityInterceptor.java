package info.archinnov.achilles.proxy;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.proxy.wrapper.CQLCounterWrapper;
import info.archinnov.achilles.type.Counter;

/**
 * ThriftEntityInterceptor
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityInterceptor<T> extends EntityInterceptor<CQLPersistenceContext, T>
{

    public CQLEntityInterceptor() {
        super.loader = new CQLEntityLoader();
        super.persister = new CQLEntityPersister();
        super.proxifier = new CQLEntityProxifier();
    }

    @Override
    protected Counter buildCounterWrapper(PropertyMeta propertyMeta)
    {
        return new CQLCounterWrapper(context, propertyMeta);
    }

}
