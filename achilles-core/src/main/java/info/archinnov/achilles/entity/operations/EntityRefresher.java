package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.proxy.EntityInterceptor;
import java.lang.reflect.Method;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityRefresher
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityRefresher<CONTEXT extends PersistenceContext> {
    private static final Logger log = LoggerFactory.getLogger(EntityRefresher.class);

    private EntityProxifier<CONTEXT> proxifier;
    private EntityLoader<CONTEXT> loader;

    public EntityRefresher() {
    }

    public EntityRefresher(EntityLoader<CONTEXT> loader, EntityProxifier<CONTEXT> proxifier) {
        this.loader = loader;
        this.proxifier = proxifier;
    }

    public <T> void refresh(CONTEXT context) throws AchillesStaleObjectStateException {
        Object primaryKey = context.getPrimaryKey();
        log.debug("Refreshing entity of class {} and primary key {}", context.getEntityClass().getCanonicalName(),
                primaryKey);

        Object entity = context.getEntity();

        EntityInterceptor<CONTEXT, Object> interceptor = proxifier.getInterceptor(entity);

        interceptor.getDirtyMap().clear();
        Set<Method> alreadyLoaded = interceptor.getAlreadyLoaded();
        alreadyLoaded.clear();
        alreadyLoaded.addAll(context.getEntityMeta().getEagerGetters());

        Object freshEntity = loader.load(context, context.getEntityClass());

        if (freshEntity == null)
        {
            throw new AchillesStaleObjectStateException("The entity '" + entity + "' with primary_key '"
                    + primaryKey + "' no longer exists in Cassandra");
        }
        interceptor.setTarget(freshEntity);
    }
}
