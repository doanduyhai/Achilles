package info.archinnov.achilles.entity.metadata;

import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import java.lang.reflect.Method;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyMetaBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMetaBuilder {
    private static final Logger log = LoggerFactory.getLogger(PropertyMetaBuilder.class);

    private PropertyType type;
    private String propertyName;
    private String entityClassName;
    private Method[] accessors;
    private ObjectMapper objectMapper;
    private CounterProperties counterProperties;

    private JoinProperties joinProperties;
    private CompoundKeyProperties compoundKeyProperties;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

    public static PropertyMetaBuilder factory() {
        return new PropertyMetaBuilder();
    }

    public PropertyMetaBuilder propertyName(String propertyName) {
        this.propertyName = propertyName;
        return this;
    }

    public PropertyMetaBuilder entityClassName(String entityClassName) {
        this.entityClassName = entityClassName;
        return this;
    }

    public PropertyMetaBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        return this;
    }

    public <K, V> PropertyMeta<K, V> build(Class<K> keyClass, Class<V> valueClass) {
        log.debug("Build propertyMeta for property {} of entity class {}", propertyName, entityClassName);

        PropertyMeta<K, V> meta = null;
        boolean singleKey = compoundKeyProperties == null ? true : false;
        meta = new PropertyMeta<K, V>();
        meta.setObjectMapper(objectMapper);
        meta.setType(type);
        meta.setPropertyName(propertyName);
        meta.setEntityClassName(entityClassName);
        meta.setKeyClass(keyClass);
        meta.setValueClass(valueClass);
        meta.setGetter(accessors[0]);
        meta.setSetter(accessors[1]);

        meta.setJoinProperties(joinProperties);
        meta.setCompoundKeyProperties(compoundKeyProperties);

        meta.setSingleKey(singleKey);
        meta.setCounterProperties(counterProperties);
        meta.setConsistencyLevels(consistencyLevels);

        return meta;
    }

    public PropertyMetaBuilder type(PropertyType type) {
        this.type = type;
        return this;
    }

    public PropertyMetaBuilder accessors(Method[] accessors) {
        this.accessors = accessors;
        return this;
    }

    public PropertyMetaBuilder multiKeyProperties(CompoundKeyProperties multiKeyProperties) {
        this.compoundKeyProperties = multiKeyProperties;
        return this;
    }

    public PropertyMetaBuilder counterProperties(CounterProperties counterProperties) {
        this.counterProperties = counterProperties;
        return this;
    }

    public PropertyMetaBuilder consistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
        return this;
    }

}
