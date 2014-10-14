package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.provider.ServiceProvider;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class PropertyMetaValues extends PropertyMetaView {

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaValues.class);

    ReflectionInvoker invoker = ReflectionInvoker.Singleton.INSTANCE.get();

    protected PropertyMetaValues(PropertyMeta meta) {
        super(meta);
    }

    public Object getPrimaryKey(Object entity) {
        log.trace("Extract primary from entity {} of class {}", entity, meta.getEntityClassName());
        if (meta.type().isId()) {
            return invoker.getPrimaryKey(entity, meta);
        } else {
            throw new IllegalStateException("Cannot get primary key on a non id field '" + meta.propertyName + "'");
        }
    }

    public Object instantiate() {
        log.trace("Instantiate new entity of class {}", meta.getEntityClassName());
        return invoker.instantiate(meta.getValueClass());
    }

    public Object getValueFromField(Object target) {
        return invoker.getValueFromField(target, meta.getField());
    }

    public void setValueToField(Object target, Object args) {
        invoker.setValueToField(target, meta.getField(), args);
    }

    public Object nullValueForCollectionAndMap() {
        Object value = null;
        if (meta.isEmptyCollectionAndMapIfNull()) {
            switch (meta.type()) {
                case LIST:
                    value = new ArrayList<>();
                    break;
                case SET:
                    value = new HashSet<>();
                    break;
                case MAP:
                    value = new HashMap<>();
                    break;
                default:
                    break;
            }
        }
        log.trace("Get null or empty collection & map {} for property {} of entity class {}", value, meta.getPropertyName(), meta.getEntityClassName());
        return value;
    }

    public Object forceLoad(Object target) {
        return invoker.getValueFromField(target, meta.getGetter());
    }

}
