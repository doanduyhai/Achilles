package info.archinnov.achilles.entity.operations.impl;

import static com.google.common.collect.Collections2.filter;
import static info.archinnov.achilles.entity.metadata.PropertyType.counterType;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder.CounterImpl;
import info.archinnov.achilles.validation.Validator;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.utils.Pair;
import com.google.common.collect.FluentIterable;

/**
 * CQLPersisterImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPersisterImpl
{
    private ReflectionInvoker invoker = new ReflectionInvoker();
    private NullJoinValuesFilter nullJoinValuesFilter = new NullJoinValuesFilter();

    public void persist(CQLPersistenceContext context)
    {
        context.pushInsertStatement();
    }

    public void persistClusteredCounter(CQLPersistenceContext context)
    {
        Object entity = context.getEntity();
        PropertyMeta counterMeta = context.getFirstMeta();
        Object counter = invoker.getValueFromField(entity, counterMeta.getGetter());
        if (counter != null)
        {
            Validator.validateTrue(
                    CounterImpl.class.isAssignableFrom(counter.getClass()),
                    "Counter property '%s' value from entity class '%s'  should be of type '%s'",
                    counterMeta.getPropertyName(), counterMeta.getEntityClassName(),
                    CounterImpl.class.getCanonicalName());
            CounterImpl counterValue = (CounterImpl) counter;
            context.pushClusteredCounterIncrementStatement(counterMeta, counterValue.get());
        }
        else
        {
            throw new IllegalStateException("Cannot insert clustered counter entity '" + entity
                    + "' with null clustered counter value");
        }

    }

    public void persistCounters(CQLPersistenceContext context, Set<PropertyMeta> counterMetas)
    {
        Object entity = context.getEntity();
        for (PropertyMeta counterMeta : counterMetas)
        {
            Object counter = invoker.getValueFromField(entity, counterMeta.getGetter());
            if (counter != null)
            {
                Validator.validateTrue(
                        CounterImpl.class.isAssignableFrom(counter.getClass()),
                        "Counter property '%s' value from entity class '%s'  should be of type '%s'",
                        counterMeta.getPropertyName(), counterMeta.getEntityClassName(),
                        CounterImpl.class.getCanonicalName());
                CounterImpl counterValue = (CounterImpl) counter;
                context.bindForSimpleCounterIncrement(counterMeta, counterValue.get());
            }
        }
    }

    public void cascadePersist(CQLEntityPersister entityPersister, CQLPersistenceContext context,
            Set<PropertyMeta> joinPMs)
    {
        Object entity = context.getEntity();
        JoinValuesExtractor extractorAndFilter = new JoinValuesExtractor(entity);

        List<Pair<List<?>, PropertyMeta>> pairList = FluentIterable
                .from(joinPMs)
                .transform(extractorAndFilter)
                .filter(nullJoinValuesFilter)
                .toImmutableList();

        doCascade(entityPersister, context, pairList);
    }

    public void ensureEntitiesExist(CQLPersistenceContext context, Set<PropertyMeta> joinPMs)
    {
        Object entity = context.getEntity();
        JoinValuesExtractor extractorAndFilter = new JoinValuesExtractor(entity);

        List<Pair<List<?>, PropertyMeta>> pairList = FluentIterable
                .from(joinPMs)
                .transform(extractorAndFilter)
                .filter(nullJoinValuesFilter)
                .toImmutableList();

        checkForExistence(context, pairList);
    }

    public void remove(CQLPersistenceContext context)
    {
        EntityMeta entityMeta = context.getEntityMeta();
        if (entityMeta.isClusteredCounter())
        {
            context.bindForClusteredCounterRemoval(entityMeta.getFirstMeta());
        }
        else
        {
            context.bindForRemoval(entityMeta.getTableName());
            removeLinkedCounters(context);
        }
    }

    protected void removeLinkedCounters(CQLPersistenceContext context)
    {
        EntityMeta entityMeta = context.getEntityMeta();

        List<PropertyMeta> allMetas = entityMeta.getAllMetasExceptIdMeta();
        Collection<PropertyMeta> proxyMetas = filter(allMetas, counterType);
        for (PropertyMeta pm : proxyMetas)
        {
            context.bindForSimpleCounterRemoval(pm);
        }
    }

    private void doCascade(CQLEntityPersister entityPersister, CQLPersistenceContext context,
            List<Pair<List<?>, PropertyMeta>> pairList)
    {
        for (Pair<List<?>, PropertyMeta> pair : pairList)
        {
            List<?> joinValues = pair.left;
            PropertyMeta joinPM = pair.right;
            for (Object joinEntity : joinValues)
            {
                CQLPersistenceContext joinContext = context.createContextForJoin(joinPM
                        .getJoinProperties()
                        .getEntityMeta(), joinEntity);
                entityPersister.persist(joinContext);
            }
        }
    }

    private void checkForExistence(CQLPersistenceContext context,
            List<Pair<List<?>, PropertyMeta>> pairList)
    {
        for (Pair<List<?>, PropertyMeta> pair : pairList)
        {
            List<?> joinValues = pair.left;
            PropertyMeta joinPM = pair.right;
            for (Object joinEntity : joinValues)
            {
                if (joinEntity != null && context.addToProcessingList(joinEntity))
                {
                    EntityMeta joinMeta = joinPM.getJoinProperties().getEntityMeta();
                    CQLPersistenceContext joinContext = context.createContextForJoin(joinMeta,
                            joinEntity);
                    boolean entityExist = joinContext.checkForEntityExistence();

                    Validator
                            .validateTrue(
                                    entityExist,
                                    "The entity '"
                                            + joinMeta.getClassName()
                                            + "' with id '"
                                            + invoker.getPrimaryKey(joinEntity, joinMeta.getIdMeta())
                                            + "' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST/CascadeType.ALL");
                }
            }
        }

    }
}
