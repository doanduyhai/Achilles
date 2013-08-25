package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.lazyType;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.util.AlreadyLoadedTransformer;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.type.Counter;
import java.lang.reflect.Method;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;

/**
 * EntityInitializer
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityInitializer
{
    private static final Logger log = LoggerFactory.getLogger(EntityInitializer.class);

    private ReflectionInvoker invoker = new ReflectionInvoker();
    private EntityProxifier<PersistenceContext> proxifier = new EntityProxifier<PersistenceContext>() {

        @Override
        public <T> EntityInterceptor<PersistenceContext, T> buildInterceptor(PersistenceContext context, T entity,
                Set<Method> alreadyLoaded) {
            return null;
        }
    };

    public <T, CONTEXT extends PersistenceContext>
            void initializeEntity(T entity, EntityMeta entityMeta,
                    EntityInterceptor<CONTEXT, T> interceptor)
    {

        log.debug("Initializing lazy fields for entity {} of class {}", entity,
                entityMeta.getClassName());

        Set<PropertyMeta> alreadyLoadedMetas = FluentIterable
                .from(interceptor.getAlreadyLoaded())
                .transform(new AlreadyLoadedTransformer(entityMeta.getGetterMetas()))
                .toImmutableSet();

        Set<PropertyMeta> allLazyMetas = FluentIterable
                .from(entityMeta.getPropertyMetas().values())
                .filter(lazyType)
                .toImmutableSet();

        Set<PropertyMeta> toBeLoadedMetas = Sets.difference(allLazyMetas, alreadyLoadedMetas);

        for (PropertyMeta propertyMeta : toBeLoadedMetas)
        {
            Object value = invoker.getValueFromField(entity, propertyMeta.getGetter());
            if (propertyMeta.isCounter())
            {
                Counter counter = (Counter) value;
                Object realObject = proxifier.getRealObject(entity);
                invoker.setValueToField(realObject, propertyMeta.getSetter(), CounterBuilder.incr(counter.get()));
            }
        }

        for (PropertyMeta propertyMeta : alreadyLoadedMetas)
        {
            if (propertyMeta.isCounter())
            {
                Object value = invoker.getValueFromField(entity, propertyMeta.getGetter());
                Counter counter = (Counter) value;
                Object realObject = proxifier.getRealObject(entity);
                invoker.setValueToField(realObject, propertyMeta.getSetter(), CounterBuilder.incr(counter.get()));
            }
        }
    }
}
