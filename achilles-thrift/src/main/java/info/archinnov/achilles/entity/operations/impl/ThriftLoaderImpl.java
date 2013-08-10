package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.logger.ThriftLoggerHelper.format;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.KeyValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftLoaderImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftLoaderImpl
{
    private static final Logger log = LoggerFactory.getLogger(ThriftLoaderImpl.class);

    private ThriftEntityMapper mapper = new ThriftEntityMapper();
    private ReflectionInvoker invoker = new ReflectionInvoker();
    private ThriftCompositeFactory compositeFactory = new ThriftCompositeFactory();
    private ThriftCompositeTransformer compositeTransformer = new ThriftCompositeTransformer();

    public <T> T load(ThriftPersistenceContext context, Class<T> entityClass)
    {
        log.trace("Loading entity of class {} with primary key {}", context
                .getEntityClass()
                .getCanonicalName(),
                context.getPrimaryKey());
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();
        T entity = null;
        if (entityMeta.isClusteredEntity())
        {
            entity = loadClusteredEntity(context, entityClass, entityMeta, primaryKey);
        }
        else
        {
            List<Pair<Composite, String>> columns = context.getEntityDao().eagerFetchEntity(
                    primaryKey);
            if (columns.size() > 0)
            {
                log.trace("Mapping data from Cassandra columns to entity");

                entity = invoker.instanciate(entityClass);
                mapper.setEagerPropertiesToEntity(primaryKey, columns, entityMeta, entity);
                invoker.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);
            }
        }
        return entity;
    }

    public <V> V loadSimpleProperty(ThriftPersistenceContext context,
            PropertyMeta<?, V> propertyMeta)
    {
        if (context.isClusteredEntity())
        {
            Object embeddedId = context.getPrimaryKey();
            Object partitionKey = context.getPartitionKey();
            PropertyMeta<?, ?> idMeta = context.getIdMeta();
            Composite composite = compositeFactory.createBaseForClusteredGet(embeddedId, idMeta);
            if (log.isTraceEnabled())
            {
                log
                        .trace(
                                "Loading simple property {} of clustered entity {} from column family {} with primary key {} and composite column name {}",
                                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(),
                                context.getEntityMeta()
                                        .getTableName(), context.getPrimaryKey(), format(composite));
            }
            Object value = context.getWideRowDao().getValue(partitionKey, composite);
            return propertyMeta.castValue(value);
        }
        else
        {
            Composite composite = compositeFactory.createBaseForGet(propertyMeta);
            if (log.isTraceEnabled())
            {
                log
                        .trace(
                                "Loading simple property {} of entity {} from column family {} with primary key {} and composite column name {}",
                                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(),
                                context.getEntityMeta()
                                        .getTableName(), context.getPrimaryKey(), format(composite));
            }
            return propertyMeta.getValueFromString(context.getEntityDao().getValue(
                    context.getPrimaryKey(), composite));
        }
    }

    public <V> List<V> loadListProperty(ThriftPersistenceContext context,
            PropertyMeta<?, V> propertyMeta)
    {
        log.trace("Loading list property {} of class {} from column family {} with primary key {}",
                propertyMeta
                        .getPropertyName(), propertyMeta.getEntityClassName(), context
                        .getEntityMeta()
                        .getTableName(),
                context.getPrimaryKey());
        List<Pair<Composite, String>> columns = fetchColumns(context, propertyMeta);
        List<V> list = null;
        if (columns.size() > 0)
        {
            list = new ArrayList<V>();
            for (Pair<Composite, String> pair : columns)
            {
                list.add(propertyMeta.getValueFromString(pair.right));
            }
        }
        return list;
    }

    public <V> Set<V> loadSetProperty(ThriftPersistenceContext context,
            PropertyMeta<?, V> propertyMeta)
    {
        log.trace("Loading set property {} of class {} from column family {} with primary key {}",
                propertyMeta
                        .getPropertyName(), propertyMeta.getEntityClassName(), context
                        .getEntityMeta()
                        .getTableName(),
                context.getPrimaryKey());
        List<Pair<Composite, String>> columns = fetchColumns(context, propertyMeta);
        Set<V> set = null;
        if (columns.size() > 0)
        {
            set = new HashSet<V>();
            for (Pair<Composite, String> pair : columns)
            {
                set.add(propertyMeta.getValueFromString(pair.right));
            }
        }
        return set;
    }

    public <K, V> Map<K, V> loadMapProperty(ThriftPersistenceContext context,
            PropertyMeta<K, V> propertyMeta)
    {
        log.trace("Loading map property {} of class {} from column family {} with primary key {}",
                propertyMeta
                        .getPropertyName(), propertyMeta.getEntityClassName(), context
                        .getEntityMeta()
                        .getTableName(),
                context.getPrimaryKey());
        List<Pair<Composite, String>> columns = fetchColumns(context, propertyMeta);
        Class<K> keyClass = propertyMeta.getKeyClass();
        Map<K, V> map = null;
        if (columns.size() > 0)
        {
            map = new HashMap<K, V>();
            for (Pair<Composite, String> pair : columns)
            {
                KeyValue<K, V> holder = propertyMeta.getKeyValueFromString(pair.right);

                map.put(keyClass.cast(holder.getKey()), holder.getValue());
            }
        }
        return map;
    }

    private <V> List<Pair<Composite, String>> fetchColumns(ThriftPersistenceContext context,
            PropertyMeta<?, V> propertyMeta)
    {

        Composite start = compositeFactory.createBaseForQuery(propertyMeta, EQUAL);
        Composite end = compositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
        if (log.isTraceEnabled())
        {
            log.trace("Fetching columns from Cassandra with column names {} / {}", format(start),
                    format(end));
        }
        List<Pair<Composite, String>> columns = context.getEntityDao().findColumnsRange(
                context.getPrimaryKey(),
                start, end, false, Integer.MAX_VALUE);
        return columns;
    }

    public <V> V loadJoinSimple(ThriftPersistenceContext context, PropertyMeta<?, V> propertyMeta,
            ThriftEntityLoader loader)
    {
        EntityMeta joinMeta = propertyMeta.joinMeta();
        PropertyMeta<?, ?> joinIdMeta = propertyMeta.joinIdMeta();

        Object joinId;
        if (context.isClusteredEntity())
        {
            joinId = retrieveJoinIdForClusteredEntity(context, propertyMeta);
        }
        else
        {
            String stringJoinId = retrieveJoinIdForEntity(context, propertyMeta);
            joinId = stringJoinId != null ? joinIdMeta.getValueFromString(stringJoinId) : null;
        }

        if (joinId != null)
        {
            ThriftPersistenceContext joinContext = context.createContextForJoin(
                    propertyMeta.getValueClass(),
                    joinMeta, joinId);
            return loader.<V> load(joinContext, propertyMeta.getValueClass());
        }
        else
        {
            return null;
        }
    }

    private <V> String retrieveJoinIdForEntity(ThriftPersistenceContext context,
            PropertyMeta<?, V> propertyMeta)
    {
        Composite composite = compositeFactory.createBaseForGet(propertyMeta);
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Loading join primary key for property {} of entity {} from column family {} with primary key {} and column name {}",
                            propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(),
                            context.getEntityMeta()
                                    .getTableName(), context.getPrimaryKey(), format(composite));
        }
        return context.getEntityDao().getValue(context.getPrimaryKey(), composite);
    }

    private Object retrieveJoinIdForClusteredEntity(ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta)
    {
        Object embeddedId = context.getPrimaryKey();
        PropertyMeta<?, ?> idMeta = context.getEntityMeta().getIdMeta();
        Object partitionKey = invoker.getPartitionKey(embeddedId, idMeta);
        Composite composite = compositeFactory.createBaseForClusteredGet(embeddedId, idMeta);
        if (log.isTraceEnabled())
        {
            log
                    .trace(
                            "Loading join primary key for property {} of clustered entity {} from column family {} with primary key {} and column name {}",
                            propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(),
                            context.getEntityMeta()
                                    .getTableName(), embeddedId, format(composite));
        }
        return context.getWideRowDao().getValue(partitionKey, composite);
    }

    private <T> T loadClusteredEntity(ThriftPersistenceContext context, Class<T> entityClass,
            EntityMeta entityMeta,
            Object primaryKey)
    {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        boolean isCounter = entityMeta.isValueless() ? false : entityMeta.getFirstMeta().isCounter();
        boolean isJoin = entityMeta.isValueless() ? false : entityMeta.getFirstMeta().isJoin();
        Composite composite = compositeFactory.createBaseForClusteredGet(primaryKey, idMeta);
        Object partitionKey = invoker.getPartitionKey(primaryKey, idMeta);

        T clusteredEntity;
        if (entityMeta.isValueless())
        {
            HColumn<Composite, Object> column = context.getWideRowDao().getColumn(partitionKey,
                    composite);
            clusteredEntity = column != null ? compositeTransformer
                    .buildClusteredEntityWithIdOnly(entityClass,
                            context, column.getName().getComponents()) : null;
        }
        else if (isCounter)
        {
            HCounterColumn<Composite> counterColumn = context.getWideRowDao().getCounterColumn(
                    partitionKey, composite);
            clusteredEntity = counterColumn != null ? compositeTransformer
                    .buildClusteredEntityWithIdOnly(entityClass,
                            context, counterColumn.getName().getComponents()) : null;
        }
        else if (isJoin)
        {
            HColumn<Composite, Object> column = context.getWideRowDao().getColumn(partitionKey,
                    composite);
            clusteredEntity = column != null ? mapper.initClusteredEntity(entityClass, idMeta,
                    primaryKey) : null;
        }
        else
        {
            HColumn<Composite, Object> column = context.getWideRowDao().getColumn(partitionKey,
                    composite);
            clusteredEntity = column != null ? compositeTransformer.buildClusteredEntity(
                    entityClass, context, column) : null;
        }
        return clusteredEntity;
    }
}
