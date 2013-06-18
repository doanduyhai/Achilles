package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.EntityInterceptor;
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

    public <T> void refresh(CONTEXT context) {
        log.debug("Refreshing entity of class {} and primary key {}", context.getEntityClass().getCanonicalName(),
                context.getPrimaryKey());

        Object entity = context.getEntity();

        EntityInterceptor<CONTEXT, Object> interceptor = proxifier.getInterceptor(entity);

        Object freshEntity = loader.load(context, context.getEntityClass());

        interceptor.getDirtyMap().clear();
        interceptor.getAlreadyLoaded().clear();
        interceptor.setTarget(freshEntity);
    }
}
