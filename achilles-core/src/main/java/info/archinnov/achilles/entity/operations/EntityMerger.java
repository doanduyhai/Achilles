package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.JoinProperties.hasCascadeMerge;
import static info.archinnov.achilles.entity.metadata.PropertyType.joinPropertyType;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.Merger;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.FluentIterable;

/**
 * EntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class EntityMerger<CONTEXT extends PersistenceContext> {

    private static final Logger log = LoggerFactory.getLogger(EntityMerger.class);

    protected Merger<CONTEXT> merger;
    protected EntityPersister<CONTEXT> persister;
    protected EntityProxifier<CONTEXT> proxifier;

    public <T> T merge(CONTEXT context, T entity) {
        log.debug("Merging entity of class {} with primary key {}", context.getEntityClass().getCanonicalName(),
                context.getPrimaryKey());

        EntityMeta entityMeta = context.getEntityMeta();

        Validator.validateNotNull(entity, "Proxy object should not be null for merge");
        Validator.validateNotNull(entityMeta, "entityMeta should not be null for merge");

        T proxy;
        if (proxifier.isProxy(entity)) {
            log.debug("Checking for dirty fields before merging");

            T realObject = proxifier.getRealObject(entity);
            context.setEntity(realObject);

            EntityInterceptor<CONTEXT, T> interceptor = proxifier.getInterceptor(entity);
            Map<Method, PropertyMeta<?, ?>> dirtyMap = interceptor.getDirtyMap();

            if (context.addToProcessingList(realObject)) {
                merger.merge(context, dirtyMap);
                List<PropertyMeta<?, ?>> joinPMs = FluentIterable.from(entityMeta.getAllMetasExceptIdMeta())
                        .filter(joinPropertyType).filter(hasCascadeMerge).toImmutableList();

                merger.cascadeMerge(this, context, joinPMs);

                interceptor.setContext(context);
                interceptor.setTarget(realObject);
            }
            proxy = entity;
        } else {
            log.debug("Persisting transient entity");

            persister.persist(context);
            proxy = proxifier.buildProxy(entity, context);
        }
        return proxy;
    }

}
