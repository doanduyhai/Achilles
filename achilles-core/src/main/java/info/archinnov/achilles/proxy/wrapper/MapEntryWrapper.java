package info.archinnov.achilles.proxy.wrapper;

import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapEntryWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class MapEntryWrapper extends AbstractWrapper implements Map.Entry<Object, Object> {
    private static final Logger log = LoggerFactory.getLogger(MapEntryWrapper.class);

    private final Map.Entry<Object, Object> target;

    public MapEntryWrapper(Map.Entry<Object, Object> target) {
        this.target = target;
    }

    @Override
    public Object getKey() {
        return this.target.getKey();
    }

    @Override
    public Object getValue() {
        if (isJoin()) {
            return proxifier.buildProxy(this.target.getValue(), joinContext(this.target.getValue()));
        } else {
            return this.target.getValue();
        }
    }

    @Override
    public Object setValue(Object value) {
        log.trace("Mark map entry property {} of entity class {} dirty upon element set",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        Object result = this.target.setValue(proxifier.unwrap(value));
        this.markDirty();
        return result;
    }

    public boolean equals(Entry<Object, Object> entry) {
        Object key = entry.getKey();
        Object value = proxifier.unwrap(entry.getValue());

        boolean keyEquals = this.target.getKey().equals(key);

        boolean valueEquals = false;
        if (this.target.getValue() == null && value == null) {
            valueEquals = true;
        } else if (this.target.getValue() != null && value != null) {
            valueEquals = this.target.getValue().equals(value);
        }

        return keyEquals && valueEquals;
    }

    @Override
    public int hashCode() {
        Object key = this.target.getKey();
        Object value = this.target.getValue();
        int result = 1;
        result = result * 31 + key.hashCode();
        result = result * 31 + (value == null ? 0 : value.hashCode());
        return result;
    }

    public Map.Entry<Object, Object> getTarget() {
        return target;
    }

}
