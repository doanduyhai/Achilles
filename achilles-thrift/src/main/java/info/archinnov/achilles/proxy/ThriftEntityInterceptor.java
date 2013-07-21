package info.archinnov.achilles.proxy;

import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.proxy.wrapper.builder.ThriftCounterWideMapWrapperBuilder;
import info.archinnov.achilles.proxy.wrapper.builder.ThriftCounterWrapperBuilder;
import info.archinnov.achilles.proxy.wrapper.builder.ThriftJoinWideMapWrapperBuilder;
import info.archinnov.achilles.proxy.wrapper.builder.ThriftWideMapWrapperBuilder;
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
    protected Object buildCounterWrapper(PropertyMeta<?, ?> propertyMeta)
    {
        Object result;
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

        result = ThriftCounterWrapperBuilder.builder(context) //
                .counterDao(counterDao)
                .columnName(comp)
                .readLevel(propertyMeta.getReadConsistencyLevel())
                .writeLevel(propertyMeta.getWriteConsistencyLevel())
                .key(rowKey)
                .build();
        return result;
    }

    @Override
    protected <K, V> Object buildWideMapWrapper(PropertyMeta<K, V> propertyMeta)
    {
        ThriftPersistenceContext thriftContext = (ThriftPersistenceContext) context;
        String columnFamilyName = context.isClusteredEntity() ? context.getEntityMeta().getTableName()
                                                             : propertyMeta.getExternalTableName();

        ThriftGenericWideRowDao wideRowDao = thriftContext.findWideRowDao(columnFamilyName);

        return ThriftWideMapWrapperBuilder //
                .builder(primaryKey, wideRowDao, propertyMeta)
                .context(thriftContext)
                .interceptor(this)
                .build();
    }

    @Override
    protected <K> Object buildCounterWideMapWrapper(PropertyMeta<K, Counter> propertyMeta)
    {
        ThriftGenericWideRowDao counterWideMapDao = context.findWideRowDao(propertyMeta
                .getExternalTableName());

        return ThriftCounterWideMapWrapperBuilder //
                .builder(primaryKey, counterWideMapDao, propertyMeta)
                .interceptor(this)
                .context(context)
                .build();
    }

    @Override
    protected <K, V> Object buildJoinWideMapWrapper(PropertyMeta<K, V> propertyMeta)
    {
        String columnFamilyName = context.isClusteredEntity() ? context.getEntityMeta().getTableName()
                                                             : propertyMeta.getExternalTableName();
        ThriftGenericWideRowDao wideRowDao = context.findWideRowDao(columnFamilyName);

        return ThriftJoinWideMapWrapperBuilder //
                .builder(primaryKey, wideRowDao, propertyMeta)
                .interceptor(this)
                .context(context)
                .build();
    }

}
