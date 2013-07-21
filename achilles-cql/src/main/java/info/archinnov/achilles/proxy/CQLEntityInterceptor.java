package info.archinnov.achilles.proxy;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
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
    protected Object buildCounterWrapper(PropertyMeta<?, ?> propertyMeta)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected <K, V> Object buildJoinWideMapWrapper(PropertyMeta<K, V> propertyMeta)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected <K> Object buildCounterWideMapWrapper(PropertyMeta<K, Counter> propertyMeta)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected <K, V> Object buildWideMapWrapper(PropertyMeta<K, V> propertyMeta)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
