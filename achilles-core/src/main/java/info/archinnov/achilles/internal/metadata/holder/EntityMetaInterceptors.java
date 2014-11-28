package info.archinnov.achilles.internal.metadata.holder;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.interceptor.AchillesInternalInterceptor;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.validation.Validator;

import java.util.List;

public class EntityMetaInterceptors extends EntityMetaView{

    protected EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();

    protected EntityMetaInterceptors(EntityMeta meta) {
        super(meta);
    }

    public void addInterceptor(Interceptor<?> interceptor) {
        meta.getInterceptors().add(interceptor);
    }

    public List<Interceptor<?>> getInterceptors() {
        return meta.getInterceptors();
    }

    public void intercept(Object entity, Event event) {
        List<Interceptor<?>> interceptors = getInterceptorsForEvent(event);
        if (interceptors.size() > 0) {
            for (Interceptor interceptor : interceptors) {
                if (AchillesInternalInterceptor.class.isAssignableFrom(interceptor.getClass())) {
                    interceptor.onEvent(entity);
                } else {
                    interceptor.onEvent(proxifier.getRealObject(entity));
                }
            }
            Validator.validateNotNull(meta.forOperations().getPrimaryKey(entity),
                    "The primary key should not be null after intercepting the event '%s'", event);
        }
    }

    protected List<Interceptor<?>> getInterceptorsForEvent(final Event event) {
        return FluentIterable.from(meta.getInterceptors()).filter(getFilterForEvent(event)).toList();

    }

    private Predicate<? super Interceptor<?>> getFilterForEvent(final Event event) {
        return new Predicate<Interceptor<?>>() {
            public boolean apply(Interceptor<?> interceptor) {
                return interceptor != null && interceptor.events() != null && interceptor.events().contains(event);
            }
        };
    }
}
