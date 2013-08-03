package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Method;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityValidator<CONTEXT extends PersistenceContext> {
    private static final Logger log = LoggerFactory.getLogger(EntityValidator.class);

    private ReflectionInvoker invoker = new ReflectionInvoker();
    private EntityProxifier<CONTEXT> proxifier;

    public EntityValidator(EntityProxifier<CONTEXT> proxifier) {
        this.proxifier = proxifier;
    }

    public void validateEntity(Object entity, Map<Class<?>, EntityMeta> entityMetaMap) {
        Validator.validateNotNull(entity, "Entity should not be null");

        Class<?> baseClass = proxifier.deriveBaseClass(entity);
        EntityMeta entityMeta = entityMetaMap.get(baseClass);
        validateEntity(entity, entityMeta);

    }

    public void validateEntity(Object entity, EntityMeta entityMeta) {
        log.debug("Validate entity {}", entity);
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

        Validator.validateNotNull(entityMeta, "The entity " + entity.getClass().getCanonicalName()
                + " is not managed by Achilles");

        Object id = invoker.getPrimaryKey(entity, idMeta);
        if (id == null) {
            throw new IllegalArgumentException("Cannot get primary key for entity "
                    + entity.getClass().getCanonicalName());
        }
        if (idMeta.isCompound()) {
            for (Method getter : idMeta.getComponentGetters()) {
                Object component = invoker.getValueFromField(id, getter);
                Validator.validateNotNull(component, "The entity " + entity.getClass().getCanonicalName()
                        + " clustered key '" + idMeta.getPropertyName() + "' components should not be null");
            }
        }
    }

    public void validateNotClusteredCounter(Object entity, Map<Class<?>, EntityMeta> entityMetaMap)
    {
        Class<?> baseClass = proxifier.deriveBaseClass(entity);
        EntityMeta entityMeta = entityMetaMap.get(baseClass);
        Validator.validateFalse(entityMeta.isClusteredCounter(), "The entity '" + entity
                + "' is a clustered counter and does not support insert/update with TTL");
    }
}
