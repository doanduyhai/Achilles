package info.archinnov.achilles.entity.metadata;

import info.archinnov.achilles.entity.metadata.transcoding.CompoundTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.ListTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.MapTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.SetTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.SimpleTranscoder;
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
    private EmbeddedIdProperties embeddedIdProperties;
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
        boolean isCompound = embeddedIdProperties == null ? false : true;
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
        meta.setEmbeddedIdProperties(embeddedIdProperties);

        meta.setCounterProperties(counterProperties);
        meta.setConsistencyLevels(consistencyLevels);
        meta.setTranscoder(determineTranscoder(isCompound));

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

    public PropertyMetaBuilder embeddedIdProperties(EmbeddedIdProperties embeddedIdProperties) {
        this.embeddedIdProperties = embeddedIdProperties;
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

    private DataTranscoder determineTranscoder(boolean isCompound)
    {
        switch (type)
        {
            case EMBEDDED_ID:
                return new CompoundTranscoder(objectMapper);
            case ID:
            case COUNTER:
            case SIMPLE:
            case LAZY_SIMPLE:
            case JOIN_SIMPLE:
                return new SimpleTranscoder(objectMapper);
            case LIST:
            case LAZY_LIST:
            case JOIN_LIST:
                return new ListTranscoder(objectMapper);
            case SET:
            case LAZY_SET:
            case JOIN_SET:
                return new SetTranscoder(objectMapper);
            case MAP:
            case LAZY_MAP:
            case JOIN_MAP:
                return new MapTranscoder(objectMapper);

            default:
                return null;
        }
    }

}
