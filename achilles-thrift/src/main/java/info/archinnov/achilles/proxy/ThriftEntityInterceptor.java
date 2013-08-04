package info.archinnov.achilles.proxy;

import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.proxy.wrapper.builder.ThriftCounterWrapperBuilder;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import me.prettyprint.hector.api.beans.Composite;

/**
 * ThriftEntityInterceptor
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityInterceptor<T> extends
        EntityInterceptor<ThriftPersistenceContext, T>
{

    private ThriftCompositeFactory thriftCompositeFactory = new ThriftCompositeFactory();
    private ReflectionInvoker invoker = new ReflectionInvoker();

    public ThriftEntityInterceptor() {
        super.loader = new ThriftEntityLoader();
        super.persister = new ThriftEntityPersister();
        super.proxifier = new ThriftEntityProxifier();
    }

    @Override
    protected Counter buildCounterWrapper(PropertyMeta<?, ?> propertyMeta)
    {
        Counter result;
        Object rowKey;
        Composite comp;
        ThriftAbstractDao counterDao;
        CounterProperties counterProperties = propertyMeta.getCounterProperties();
        PropertyMeta<?, ?> idMeta = counterProperties.getIdMeta();
        if (context.isClusteredEntity())
        {
            rowKey = invoker.getPartitionKey(primaryKey, idMeta);
            comp = thriftCompositeFactory.createBaseForClusteredGet(primaryKey,
                    idMeta);
            counterDao = context.getWideRowDao();
        }
        else
        {
            rowKey = thriftCompositeFactory.createKeyForCounter(counterProperties.getFqcn(),
                    primaryKey, idMeta);
            comp = thriftCompositeFactory.createBaseForCounterGet(propertyMeta);
            counterDao = context.getCounterDao();
        }

        ConsistencyLevel readLevel = context.getReadConsistencyLevel().isPresent() ? context
                .getReadConsistencyLevel().get() : propertyMeta.getReadConsistencyLevel();
        ConsistencyLevel writeLevel = context.getWriteConsistencyLevel().isPresent() ? context
                .getWriteConsistencyLevel().get() : propertyMeta.getWriteConsistencyLevel();

        result = ThriftCounterWrapperBuilder.builder(context) //
                .counterDao(counterDao)
                .columnName(comp)
                .readLevel(readLevel)
                .writeLevel(writeLevel)
                .key(rowKey)
                .build();
        return result;
    }
}
