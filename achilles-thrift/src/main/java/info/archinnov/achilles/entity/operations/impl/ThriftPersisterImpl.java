package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.validation.Validator;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftPersisterImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPersisterImpl
{
    private static final Logger log = LoggerFactory.getLogger(ThriftPersisterImpl.class);

    private MethodInvoker invoker = new MethodInvoker();
    private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();

    private ThriftCompositeFactory thriftCompositeFactory = new ThriftCompositeFactory();

    public void batchPersistSimpleProperty(ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta)
    {
        Composite name = thriftCompositeFactory.createForBatchInsertSingleValue(propertyMeta);
        String value = propertyMeta.writeValueToString(invoker.getValueFromField(
                context.getEntity(), propertyMeta.getGetter()));
        if (value != null)
        {
            if (log.isTraceEnabled())
            {
                log
                        .trace("Batch persisting simple property {} from entity of class {} and primary key {} with column name {}",
                                propertyMeta.getPropertyName(), context
                                        .getEntityClass()
                                        .getCanonicalName(), context.getPrimaryKey(), format(name));
            }
            context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
                    context.getTttO(),
                    context.getEntityMutator(context.getTableName()));
        }
    }

    public <V> void batchPersistList(List<V> list, ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta)
    {
        int count = 0;
        for (V value : list)
        {
            Composite name = thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta,
                    count);

            String stringValue = propertyMeta.writeValueToString(value);
            if (stringValue != null)
            {
                if (log.isTraceEnabled())
                {
                    log
                            .trace("Batch persisting list property {} from entity of class {} and primary key {} with column name {}",
                                    propertyMeta.getPropertyName(), context
                                            .getEntityClass()
                                            .getCanonicalName(), context.getPrimaryKey(),
                                    format(name));
                }
                context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
                        stringValue, context.getTttO(),
                        context.getEntityMutator(context.getTableName()));
            }
            count++;
        }
    }

    public <V> void batchPersistSet(Set<V> set, ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta)
    {
        for (V value : set)
        {
            Composite name = thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta,
                    value.hashCode());

            String stringValue = propertyMeta.writeValueToString(value);
            if (stringValue != null)
            {
                if (log.isTraceEnabled())
                {
                    log
                            .trace("Batch persisting set property {} from entity of class {} and primary key {} with column name {}",
                                    propertyMeta.getPropertyName(), context
                                            .getEntityClass()
                                            .getCanonicalName(), context.getPrimaryKey(),
                                    format(name));
                }
                context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
                        stringValue, context.getTttO(),
                        context.getEntityMutator(context.getTableName()));
            }
        }
    }

    public <K, V> void batchPersistMap(Map<K, V> map, ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta)
    {
        for (Entry<K, V> entry : map.entrySet())
        {
            Composite name = thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta,
                    entry.getKey().hashCode());

            String value = propertyMeta.writeValueToString(new KeyValue<K, V>(entry.getKey(), entry
                    .getValue()));

            if (log.isTraceEnabled())
            {
                log
                        .trace("Batch persisting map property {} from entity of class {} and primary key {} with column name {}",
                                propertyMeta.getPropertyName(), context
                                        .getEntityClass()
                                        .getCanonicalName(), context.getPrimaryKey(), format(name));
            }
            context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
                    context.getTttO(),
                    context.getEntityMutator(context.getTableName()));
        }
    }

    public <V> void batchPersistJoinEntity(ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta, V joinEntity, ThriftEntityPersister persister)
    {
        JoinProperties joinProperties = propertyMeta.getJoinProperties();
        PropertyMeta<?, ?> idMeta = propertyMeta.joinIdMeta();

        Object joinId = invoker.getPrimaryKey(joinEntity, idMeta);
        Validator.validateNotNull(joinId, "Primary key for join entity '" + joinEntity
                + "' should not be null");
        String joinIdString = idMeta.writeValueToString(joinId);

        Composite joinComposite = thriftCompositeFactory
                .createForBatchInsertSingleValue(propertyMeta);
        context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), joinComposite,
                joinIdString, context.getTttO(), context.getEntityMutator(context.getTableName()));

        ThriftPersistenceContext joinPersistenceContext = context
                .newPersistenceContext(propertyMeta.joinMeta(), proxifier.unproxy(joinEntity));

        if (log.isTraceEnabled())
        {
            log
                    .trace("Batch persisting join primary key for property {} from entity of class {} and primary key {} with column name {}",
                            propertyMeta.getPropertyName(), context
                                    .getEntityClass()
                                    .getCanonicalName(), context.getPrimaryKey(),
                            format(joinComposite));
        }
        persister.cascadePersistOrEnsureExists(joinPersistenceContext, joinEntity, joinProperties);
    }

    public <V> void batchPersistJoinCollection(ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta, Collection<V> joinCollection,
            ThriftEntityPersister persister)
    {
        JoinProperties joinProperties = propertyMeta.getJoinProperties();
        EntityMeta joinEntityMeta = joinProperties.getEntityMeta();
        PropertyMeta<?, ?> joinIdMeta = joinEntityMeta.getIdMeta();
        int count = 0;
        for (V joinEntity : joinCollection)
        {
            Composite name = thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta,
                    count);

            Object joinEntityId = invoker.getValueFromField(joinEntity, joinIdMeta.getGetter());

            String joinEntityIdStringValue = joinIdMeta.writeValueToString(joinEntityId);
            if (joinEntityIdStringValue != null)
            {
                if (log.isTraceEnabled())
                {
                    log
                            .trace("Batch persisting join primary keys for property {} from entity of class {} and primary key {} with column name {}",
                                    propertyMeta.getPropertyName(), context
                                            .getEntityClass()
                                            .getCanonicalName(), context.getPrimaryKey(),
                                    format(name));
                }
                context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name,
                        joinEntityIdStringValue, context.getTttO(),
                        context.getEntityMutator(context.getTableName()));

                ThriftPersistenceContext joinPersistenceContext = context
                        .newPersistenceContext(propertyMeta.joinMeta(),
                                proxifier.unproxy(joinEntity));

                persister.cascadePersistOrEnsureExists(joinPersistenceContext, joinEntity,
                        joinProperties);
            }
            count++;
        }
    }

    public <K, V> void batchPersistJoinMap(ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta, Map<K, V> joinMap, ThriftEntityPersister persiter)
    {
        JoinProperties joinProperties = propertyMeta.getJoinProperties();
        EntityMeta joinEntityMeta = joinProperties.getEntityMeta();
        PropertyMeta<?, ?> idMeta = joinEntityMeta.getIdMeta();

        for (Entry<K, V> entry : joinMap.entrySet())
        {
            Composite name = thriftCompositeFactory.createForBatchInsertMultiValue(propertyMeta,
                    entry.getKey().hashCode());

            V joinEntity = entry.getValue();
            Object joinEntityId = invoker.getValueFromField(joinEntity, idMeta.getGetter());
            String joinEntityIdStringValue = idMeta.writeValueToString(joinEntityId);

            String value = propertyMeta.writeValueToString(new KeyValue<K, String>(entry.getKey(),
                    joinEntityIdStringValue));
            context.getEntityDao().insertColumnBatch(context.getPrimaryKey(), name, value,
                    context.getTttO(),
                    context.getEntityMutator(context.getTableName()));

            ThriftPersistenceContext joinPersistenceContext = context
                    .newPersistenceContext(propertyMeta.joinMeta(), proxifier.unproxy(joinEntity));

            if (log.isTraceEnabled())
            {
                log
                        .trace("Batch persisting join primary keys for property {} from entity of class {} and primary key {} with column name {}",
                                propertyMeta.getPropertyName(), context
                                        .getEntityClass()
                                        .getCanonicalName(), context.getPrimaryKey(), format(name));
            }
            persiter.cascadePersistOrEnsureExists(joinPersistenceContext, joinEntity,
                    joinProperties);
        }
    }

    public void removeEntityBatch(ThriftPersistenceContext context)
    {
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();
        if (!context.isWideRow())
        {
            log.trace("Batch removing wide row of class {} and primary key {}", context
                    .getEntityClass()
                    .getCanonicalName(), context.getPrimaryKey());
            Mutator<Object> entityMutator = context.getEntityMutator(entityMeta.getTableName());
            context.getEntityDao().removeRowBatch(primaryKey, entityMutator);
        }
    }

    public void remove(ThriftPersistenceContext context)
    {
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        if (context.isWideRow())
        {
            log.trace("Batch removing wide row of class {} and primary key {}", context
                    .getEntityClass()
                    .getCanonicalName(), context.getPrimaryKey());
            Mutator<Object> wideRowMutator = context.getWideRowMutator(entityMeta.getTableName());
            context.getWideRowDao().removeRowBatch(primaryKey, wideRowMutator);
        }
        else
        {

            log.trace("Batch removing entity of class {} and primary key {}", context
                    .getEntityClass()
                    .getCanonicalName(), context.getPrimaryKey());

            Mutator<Object> entityMutator = context.getEntityMutator(entityMeta.getTableName());
            context.getEntityDao().removeRowBatch(primaryKey, entityMutator);
            for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
            {
                PropertyMeta<?, ?> propertyMeta = entry.getValue();
                if (propertyMeta.isWideMap())
                {

                    if (propertyMeta.isCounter())
                    {
                        removeCounterWideMap(context, propertyMeta);
                    }
                    else
                    {
                        removeWideMap(context, primaryKey, propertyMeta);
                    }
                }
                if (propertyMeta.isCounter())
                {
                    removeSimpleCounter(context, propertyMeta);
                }
            }
        }
    }

    private void removeWideMap(ThriftPersistenceContext context, Object primaryKey,
            PropertyMeta<?, ?> propertyMeta)
    {
        log.trace("Batch removing wideMap property {} of class {} and primary key {}",
                propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
                context.getPrimaryKey());

        String externalColumnFamilyName = propertyMeta.getExternalTableName();
        ThriftGenericWideRowDao findColumnFamilyDao = context
                .findWideRowDao(externalColumnFamilyName);
        findColumnFamilyDao.removeRowBatch(primaryKey,
                context.getWideRowMutator(externalColumnFamilyName));
    }

    public void removePropertyBatch(ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta)
    {
        Composite start = thriftCompositeFactory.createBaseForQuery(propertyMeta,
                ComponentEquality.EQUAL);
        Composite end = thriftCompositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);

        if (log.isTraceEnabled())
        {
            log
                    .trace("Batch removing property {} of class {} and primary key {} with column names {}  / {}",
                            propertyMeta.getPropertyName(), context
                                    .getEntityClass()
                                    .getCanonicalName(), context.getPrimaryKey(), format(start),
                            format(end));
        }
        context.getEntityDao().removeColumnRangeBatch(context.getPrimaryKey(), start, end,
                context.getEntityMutator(context.getTableName()));
    }

    private void removeSimpleCounter(ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta)
    {
        Composite keyComp = thriftCompositeFactory.createKeyForCounter(propertyMeta.fqcn(),
                context.getPrimaryKey(), propertyMeta.counterIdMeta());
        Composite com = thriftCompositeFactory.createForBatchInsertSingleCounter(propertyMeta);

        log.trace("Batch removing counter property {} of class {} and primary key {}",
                propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
                context.getPrimaryKey());

        context.getCounterDao().removeCounterBatch(keyComp, com, context.getCounterMutator());
    }

    private void removeCounterWideMap(ThriftPersistenceContext context,
            PropertyMeta<?, ?> propertyMeta)
    {

        log.trace("Batch removing counter wideMap property {} of class {} and primary key {}",
                propertyMeta.getPropertyName(), context.getEntityClass().getCanonicalName(),
                context.getPrimaryKey());

        Composite keyComp = thriftCompositeFactory.createKeyForCounter(propertyMeta.fqcn(),
                context.getPrimaryKey(), propertyMeta.counterIdMeta());
        context.getCounterDao().removeCounterRowBatch(keyComp, context.getCounterMutator());
    }
}
