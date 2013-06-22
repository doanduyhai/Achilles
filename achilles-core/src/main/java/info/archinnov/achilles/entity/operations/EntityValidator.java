package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.MethodInvoker;
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

    private MethodInvoker invoker = new MethodInvoker();
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
        if (!idMeta.isSingleKey()) {
            for (Method getter : idMeta.getComponentGetters()) {
                Object component = invoker.getValueFromField(id, getter);
                Validator.validateNotNull(component, "The entity " + entity.getClass().getCanonicalName()
                        + " clustered key '" + idMeta.getPropertyName() + "' components should not be null");
            }
        }
    }

    public void validateNotWideRow(Object entity, Map<Class<?>, EntityMeta> entityMetaMap) {
        log.debug("Validate entity {} is not a wide row", entity);

        Validator.validateNotNull(entity, "Entity should not be null");

        Class<?> baseClass = proxifier.deriveBaseClass(entity);
        EntityMeta entityMeta = entityMetaMap.get(baseClass);

        if (entityMeta.isWideRow()) {
            throw new IllegalArgumentException("This operation is not allowed for the wide row '"
                    + entity.getClass().getCanonicalName());
        }
    }

    public void validateNoPendingBatch(PersistenceContext context) {
        log.debug("Validate no pending batch");
        Validator
                .validateFalse(
                        context.isBatchMode(),
                        "Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
    }
}
