package info.archinnov.achilles.proxy;

import info.archinnov.achilles.context.ThriftPersistenceContext;
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
public class ThriftEntityInterceptorBuilder<T>
{
    private static final Logger log = LoggerFactory.getLogger(ThriftEntityInterceptorBuilder.class);

    private T target;
    private Set<Method> alreadyLoaded = new HashSet<Method>();
    private ThriftPersistenceContext context;

    public static <T> ThriftEntityInterceptorBuilder<T> builder(ThriftPersistenceContext context,
            T entity)
    {
        return new ThriftEntityInterceptorBuilder<T>(context, entity);
    }

    public ThriftEntityInterceptorBuilder(ThriftPersistenceContext context, T entity) {
        Validator.validateNotNull(context, "PersistenceContext for interceptor should not be null");
        Validator.validateNotNull(entity, "Target entity for interceptor should not be null");
        this.context = context;
        this.target = entity;
    }

    public ThriftEntityInterceptor<T> build()
    {
        log.debug("Build interceptor for entity of class {}", context
                .getEntityMeta()
                .getClassName());

        ThriftEntityInterceptor<T> interceptor = new ThriftEntityInterceptor<T>();

        EntityMeta entityMeta = context.getEntityMeta();

        String className = context.getEntityClass().getCanonicalName();
        Validator.validateNotNull(target, "Target object for interceptor of '%s' should not be null", className);
        Validator.validateNotNull(entityMeta.getGetterMetas(),
                "Getters metadata for interceptor of '%s' should not be null", className);
        Validator.validateNotNull(entityMeta.getSetterMetas(),
                "Setters metadata for interceptor of '%s' should not be null", className);
        if (entityMeta.isClusteredEntity())
        {
            Validator.validateNotNull(context.getWideRowDao(), "Column Family Dao for '%s' should not be null",
                    className);
        }
        else
        {
            Validator.validateNotNull(context.getEntityDao(), "Entity dao for '%s' should not be null", className);
        }
        PropertyMeta idMeta = entityMeta.getIdMeta();
        Validator.validateNotNull(idMeta, "Id metadata for '%s' should not be null", className);

        interceptor.setTarget(target);
        interceptor.setContext(context);
        interceptor.setGetterMetas(entityMeta.getGetterMetas());
        interceptor.setSetterMetas(entityMeta.getSetterMetas());
        interceptor.setIdGetter(idMeta.getGetter());
        interceptor.setIdSetter(idMeta.getSetter());

        if (context.isLoadEagerFields())
        {
            alreadyLoaded.addAll(entityMeta.getEagerGetters());
        }

        interceptor.setAlreadyLoaded(alreadyLoaded);
        interceptor.setDirtyMap(new HashMap<Method, PropertyMeta>());

        interceptor.setPrimaryKey(context.getPrimaryKey());

        return interceptor;
    }

    public ThriftEntityInterceptorBuilder<T> alreadyLoaded(Set<Method> alreadyLoaded)
    {
        this.alreadyLoaded = alreadyLoaded;
        return this;
    }
}
