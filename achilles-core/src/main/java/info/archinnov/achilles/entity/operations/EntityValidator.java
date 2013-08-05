package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.validation.Validator;
import java.util.List;
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

        Validator.validateNotNull(entityMeta, "The entity %s is not managed by Achilles", entity.getClass()
                .getCanonicalName());

        Object id = invoker.getPrimaryKey(entity, idMeta);
        if (id == null) {
            throw new IllegalArgumentException("Cannot get primary key for entity "
                    + entity.getClass().getCanonicalName());
        }
        validatePrimaryKey(idMeta, id);
    }

    public void validatePrimaryKey(PropertyMeta<?, ?> idMeta, Object primaryKey) {
        if (idMeta.isEmbeddedId()) {
            List<Object> components = idMeta.encodeToComponents(primaryKey);
            for (Object component : components) {
                Validator.validateNotNull(component, "The clustered key '%s' components should not be null",
                        idMeta.getPropertyName());
            }
        }
    }

    public void validateNotClusteredCounter(Object entity, Map<Class<?>, EntityMeta> entityMetaMap)
    {
        Class<?> baseClass = proxifier.deriveBaseClass(entity);
        EntityMeta entityMeta = entityMetaMap.get(baseClass);
        Validator.validateFalse(entityMeta.isClusteredCounter(),
                "The entity '%s' is a clustered counter and does not support insert/update with TTL", entity);
    }
}
