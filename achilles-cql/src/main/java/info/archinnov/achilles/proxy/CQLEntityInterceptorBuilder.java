package info.archinnov.achilles.proxy;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftEntityInterceptorBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityInterceptorBuilder<T> {
    private static final Logger log = LoggerFactory.getLogger(CQLEntityInterceptorBuilder.class);

    private T target;
    private Set<Method> alreadyLoaded = new HashSet<Method>();
    private CQLPersistenceContext context;

    public static <T> CQLEntityInterceptorBuilder<T> builder(CQLPersistenceContext context, T entity) {
        return new CQLEntityInterceptorBuilder<T>(context, entity);
    }

    public CQLEntityInterceptorBuilder(CQLPersistenceContext context, T entity) {
        Validator.validateNotNull(context, "PersistenceContext for interceptor should not be null");
        Validator.validateNotNull(entity, "Target entity for interceptor should not be null");
        this.context = context;
        this.target = entity;
    }

    public CQLEntityInterceptor<T> build() {
        log.debug("Build interceptor for entity of class {}", context.getEntityMeta().getClassName());

        CQLEntityInterceptor<T> interceptor = new CQLEntityInterceptor<T>();

        EntityMeta entityMeta = context.getEntityMeta();

        String className = context.getEntityClass().getCanonicalName();
        Validator.validateNotNull(target, "Target object for interceptor of '%s' should not be null", className);
        Validator.validateNotNull(entityMeta.getGetterMetas(),
                "Getters metadata for interceptor of '%s' should not be null", className);
        Validator.validateNotNull(entityMeta.getSetterMetas(),
                "Setters metadata for interceptor of '%s' should not be null", className);
        Validator.validateNotNull(entityMeta.getIdMeta(), "Id metadata for '%s' should not be null", className);

        interceptor.setTarget(target);
        interceptor.setContext(context);
        interceptor.setGetterMetas(entityMeta.getGetterMetas());
        interceptor.setSetterMetas(entityMeta.getSetterMetas());
        interceptor.setIdGetter(entityMeta.getIdMeta().getGetter());
        interceptor.setIdSetter(entityMeta.getIdMeta().getSetter());

        if (context.isLoadEagerFields() && alreadyLoaded.isEmpty()) {
            alreadyLoaded.addAll(entityMeta.getEagerGetters());
        }
        interceptor.setAlreadyLoaded(alreadyLoaded);
        interceptor.setDirtyMap(new HashMap<Method, PropertyMeta<?, ?>>());
        interceptor.setPrimaryKey(context.getPrimaryKey());

        return interceptor;
    }

    public CQLEntityInterceptorBuilder<T> alreadyLoaded(Set<Method> alreadyLoaded) {
        this.alreadyLoaded = alreadyLoaded;
        return this;
    }
}
