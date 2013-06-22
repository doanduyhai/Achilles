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
    private CompoundKeyProperties multiKeyProperties;
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
        boolean singleKey = multiKeyProperties == null ? true : false;
        switch (type) {
            case SIMPLE:
            case LIST:
            case SET:
            case LAZY_SIMPLE:
            case LAZY_LIST:
            case LAZY_SET:
            case JOIN_SIMPLE:
            case COUNTER:
            case MAP:
            case LAZY_MAP:
            case WIDE_MAP:
            case JOIN_WIDE_MAP:
            case COUNTER_WIDE_MAP:
            case COMPOUND_ID:
                meta = new PropertyMeta<K, V>();
                break;

            default:
                throw new IllegalStateException("The type '" + type + "' is not supported for PropertyMeta builder");
        }

        meta.setObjectMapper(objectMapper);
        meta.setType(type);
        meta.setPropertyName(propertyName);
        meta.setEntityClassName(entityClassName);
        meta.setKeyClass(keyClass);
        meta.setValueClass(valueClass);
        meta.setGetter(accessors[0]);
        meta.setSetter(accessors[1]);

        meta.setJoinProperties(joinProperties);
        meta.setMultiKeyProperties(multiKeyProperties);

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
        this.multiKeyProperties = multiKeyProperties;
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
