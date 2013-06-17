package info.archinnov.achilles.entity.operations.impl;

import static com.google.common.collect.Collections2.*;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.proxy.MethodInvoker;
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
    private MethodInvoker invoker = new MethodInvoker();
    private NullJoinValuesFilter nullJoinValuesFilter = new NullJoinValuesFilter();

    public void persist(CQLPersistenceContext context)
    {
        context.bindForInsert();
    }

    public void cascadePersist(CQLEntityPersister entityPersister, CQLPersistenceContext context,
            Set<PropertyMeta<?, ?>> joinPMs)
    {
        Object entity = context.getEntity();
        JoinValuesExtractor extractorAndFilter = new JoinValuesExtractor(entity);

        List<Pair<List<?>, PropertyMeta<?, ?>>> pairList = FluentIterable
                .from(joinPMs)
                .transform(extractorAndFilter)
                .filter(nullJoinValuesFilter)
                .toImmutableList();

        doCascade(entityPersister, context, pairList);
    }

    public void ensureEntitiesExist(CQLPersistenceContext context, Set<PropertyMeta<?, ?>> joinPMs)
    {
        Object entity = context.getEntity();
        JoinValuesExtractor extractorAndFilter = new JoinValuesExtractor(entity);

        List<Pair<List<?>, PropertyMeta<?, ?>>> pairList = FluentIterable
                .from(joinPMs)
                .transform(extractorAndFilter)
                .filter(nullJoinValuesFilter)
                .toImmutableList();

        checkForExistence(context, pairList);
    }

    public void remove(CQLPersistenceContext context)
    {
        EntityMeta entityMeta = context.getEntityMeta();
        context.bindForRemoval(entityMeta.getTableName(), entityMeta.getWriteConsistencyLevel());
        removeLinkedTables(context);
    }

    protected void removeLinkedTables(CQLPersistenceContext context)
    {
        EntityMeta entityMeta = context.getEntityMeta();

        List<PropertyMeta<?, ?>> allMetas = entityMeta.getAllMetas();
        Collection<PropertyMeta<?, ?>> proxyMetas = filter(allMetas, isProxyType);
        for (PropertyMeta<?, ?> pm : proxyMetas)
        {
            if (pm.type() == COUNTER)
            {
                context.bindForSimpleCounterRemoval(entityMeta, pm, context.getPrimaryKey());
            }
            else
            {
                context.bindForRemoval(pm.getExternalTableName(), pm.getWriteConsistencyLevel());
            }

        }
    }

    private void doCascade(CQLEntityPersister entityPersister, CQLPersistenceContext context,
            List<Pair<List<?>, PropertyMeta<?, ?>>> pairList)
    {
        for (Pair<List<?>, PropertyMeta<?, ?>> pair : pairList)
        {
            List<?> joinValues = pair.left;
            PropertyMeta<?, ?> joinPM = pair.right;
            for (Object joinEntity : joinValues)
            {
                CQLPersistenceContext joinContext = context.newPersistenceContext(joinPM
                        .getJoinProperties()
                        .getEntityMeta(), joinEntity);
                entityPersister.persist(joinContext);
            }
        }
    }

    private void checkForExistence(CQLPersistenceContext context,
            List<Pair<List<?>, PropertyMeta<?, ?>>> pairList)
    {
        for (Pair<List<?>, PropertyMeta<?, ?>> pair : pairList)
        {
            List<?> joinValues = pair.left;
            PropertyMeta<?, ?> joinPM = pair.right;
            for (Object joinEntity : joinValues)
            {
                EntityMeta joinMeta = joinPM.getJoinProperties().getEntityMeta();
                CQLPersistenceContext joinContext = context.newPersistenceContext(joinMeta,
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
