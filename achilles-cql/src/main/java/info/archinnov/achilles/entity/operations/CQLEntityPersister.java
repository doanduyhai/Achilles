package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.JoinProperties.hasCascadePersist;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.CQLPersisterImpl;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;

/**
 * CQLPersisterImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityPersister implements EntityPersister<CQLPersistenceContext>
{
    private static final Logger log = LoggerFactory.getLogger(CQLEntityPersister.class);

    private CQLPersisterImpl persisterImpl = new CQLPersisterImpl();

    @Override
    public void persist(CQLPersistenceContext context)
    {
        EntityMeta entityMeta = context.getEntityMeta();

        Object entity = context.getEntity();
        if (context.addToProcessingList(entity))
        {
            log.debug("Persisting transient entity {}", entity);

            if (entityMeta.isClusteredCounter())
            {
                persisterImpl.persistClusteredCounter(context);
            }
            else
            {
                persistEntity(context, entityMeta);
            }
        }
    }

    private void persistEntity(CQLPersistenceContext context, EntityMeta entityMeta)
    {
        persisterImpl.persist(context);

        List<PropertyMeta<?, ?>> allMetas = entityMeta.getAllMetasExceptIdMeta();

        Set<PropertyMeta<?, ?>> joinPMsWithCascade = FluentIterable
                .from(allMetas)
                .filter(joinPropertyType)
                .filter(hasCascadePersist)
                .toImmutableSet();

        persisterImpl.cascadePersist(this, context, joinPMsWithCascade);

        if (context.getConfigContext().isEnsureJoinConsistency())
        {
            Set<PropertyMeta<?, ?>> joinPMs = FluentIterable
                    .from(allMetas)
                    .filter(joinPropertyType)
                    .toImmutableSet();

            Set<PropertyMeta<?, ?>> ensureExistsPMs = Sets.difference(joinPMs,
                    joinPMsWithCascade);

            log.debug("Consistency check for join entity of class {} and primary key {} ",
                    context.getEntityClass().getCanonicalName(), context.getPrimaryKey());

            persisterImpl.ensureEntitiesExist(context, ensureExistsPMs);
        }

        Set<PropertyMeta<?, ?>> counterMetas = FluentIterable
                .from(allMetas)
                .filter(counterType)
                .toImmutableSet();

        persisterImpl.persistCounters(context, counterMetas);
    }

    @Override
    public void remove(CQLPersistenceContext context)
    {
        CQLPersistenceContext cqlContext = context;
        persisterImpl.remove(cqlContext);
    }
}
