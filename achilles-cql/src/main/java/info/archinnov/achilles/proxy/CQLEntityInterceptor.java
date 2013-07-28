package info.archinnov.achilles.proxy;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;

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
    protected Object buildCounterWrapper(PropertyMeta<?, ?> propertyMeta)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
