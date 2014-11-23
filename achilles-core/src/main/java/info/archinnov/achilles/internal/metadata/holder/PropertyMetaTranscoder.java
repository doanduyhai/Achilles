package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.persistence.operations.InternalCounterImpl;
import info.archinnov.achilles.internal.validation.Validator;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertyMetaTranscoder extends PropertyMetaView {

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaTranscoder.class);

    protected PropertyMetaTranscoder(PropertyMeta meta) {
        super(meta);
    }

    public Object decodeFromComponents(List<?> components) {
        if (log.isTraceEnabled()) {
            log.trace("Decode CQL components {} into compound primary key {} for entity class {}", components, meta.getPropertyName(), meta.getEntityClassName());
        }
        Validator.validateTrue(meta.type() == PropertyType.EMBEDDED_ID, "Cannot decode components '%s' for the property '%s' which is not a compound primary key", components, meta.propertyName);
        if (CollectionUtils.isEmpty(components)) {
            return components;
        }
        final Object newInstance = meta.forValues().instantiate();
        final List<PropertyMeta> propertyMetas = meta.getEmbeddedIdProperties().propertyMetas;

        Validator.validateTrue(components.size() == propertyMetas.size(), "There should be exactly '%s' Cassandra columns to decode into an '%s' instance", propertyMetas.size(), newInstance.getClass().getCanonicalName());

        for (int i = 0; i < propertyMetas.size(); i++) {
            final PropertyMeta componentMeta = propertyMetas.get(i);
            final Object decodedValue = componentMeta.forTranscoding().decodeFromCassandra(components.get(i));
            componentMeta.forValues().setValueToField(newInstance,decodedValue);
        }
        return newInstance;
    }

    public Object decodeFromCassandra(Object fromCassandra) {
        if (log.isTraceEnabled()) {
            log.trace("Decode Cassandra value {} into Java for property {} of entity class {}", fromCassandra, meta.getPropertyName(), meta.getEntityClassName());
        }
        switch (meta.type()) {
            case SIMPLE:
            case ID:
                return meta.getSimpleCodec().decode(fromCassandra);
            case LIST:
                return meta.getListCodec().decode((List) fromCassandra);
            case SET:
                return meta.getSetCodec().decode((Set) fromCassandra);
            case MAP:
                return meta.getMapCodec().decode((Map) fromCassandra);
            default:
                throw new AchillesException(String.format("Cannot decode value '%s' from CQL for property '%s' of type '%s'", fromCassandra, meta.propertyName, meta.type().name()));
        }
    }

    public <T> T encodeToCassandra(Object fromJava) {
        if (log.isTraceEnabled()) {
            log.trace("Encode Java value {} into Cassandra for property {} of entity class {}", fromJava, meta.getPropertyName(), meta.getEntityClassName());
        }
        switch (meta.type()) {
            case SIMPLE:
            case ID:
                return (T)meta.getSimpleCodec().encode(fromJava);
            case LIST:
                return (T)meta.getListCodec().encode((List) fromJava);
            case SET:
                return (T)meta.getSetCodec().encode((Set) fromJava);
            case MAP:
                return (T)meta.getMapCodec().encode((Map) fromJava);
            case COUNTER:
                return (T)((InternalCounterImpl) fromJava).getInternalCounterDelta();
            default:
                throw new AchillesException(String.format("Cannot encode value '%s' to CQL for property '%s' of type '%s'",fromJava, meta.propertyName, meta.type().name()));
        }
    }

    public Object getAndEncodeValueForCassandra(Object entity) {
        if (log.isTraceEnabled()) {
            log.trace("Get and encode Java value into Cassandra for property {} of entity class {} from entity ", meta.getPropertyName(), meta.getEntityClassName(), entity);
        }
        Object value = meta.forValues().getValueFromField(entity);
        if (value != null) {
            return encodeToCassandra(value);
        } else {
            return null;
        }
    }

    public List<Object> encodeToComponents(Object compoundKey, boolean onlyPartitionComponents) {
        log.trace("Encode compound primary key {} to CQL components with 'onlyPartitionComponents' : {}", compoundKey, onlyPartitionComponents);
        Validator.validateTrue(meta.type() == PropertyType.EMBEDDED_ID, "Cannot encode object '%s' for the property '%s' which is not a compound primary key", compoundKey, meta.propertyName);
        List<Object> encoded = new ArrayList<>();
        if (compoundKey == null) {
            return encoded;
        }

        if (onlyPartitionComponents) {
            for (PropertyMeta partitionKeyMeta : meta.getEmbeddedIdProperties().getPartitionComponents().propertyMetas) {
                encoded.add(partitionKeyMeta.forTranscoding().getAndEncodeValueForCassandra(compoundKey));
            }
        } else {
            for (PropertyMeta partitionKeyMeta : meta.getEmbeddedIdProperties().propertyMetas) {
                encoded.add(partitionKeyMeta.forTranscoding().getAndEncodeValueForCassandra(compoundKey));
            }
        }
        return encoded;
    }

    List<Object> encodePartitionComponents(List<Object> rawPartitionComponents) {
        log.trace("Encode {} to CQL partition components", rawPartitionComponents);
        Validator.validateTrue(meta.type() == PropertyType.EMBEDDED_ID, "Cannot encode partition components '%s' for the property '%s' which is not a compound primary key", rawPartitionComponents, meta.propertyName);
        final List<PropertyMeta> partitionMetas = meta.getEmbeddedIdProperties().getPartitionComponents().propertyMetas;
        Validator.validateTrue(rawPartitionComponents.size() <= partitionMetas.size(),"There should be no more than '%s' partition components to be encoded for class '%s'", rawPartitionComponents, meta.getEntityClassName());
        return encodeElements(rawPartitionComponents, partitionMetas);
    }

    List<Object> encodePartitionComponentsIN(List<Object> rawPartitionComponentsIN) {
        log.trace("Encode {} to CQL partition components IN", rawPartitionComponentsIN);
        Validator.validateTrue(meta.type() == PropertyType.EMBEDDED_ID, "Cannot encode partition components '%s' for the property '%s' which is not a compound primary key", rawPartitionComponentsIN, meta.propertyName);
        final List<PropertyMeta> partitionMetas = meta.getEmbeddedIdProperties().getPartitionComponents().propertyMetas;
        final PropertyMeta lastPartitionComponentMeta = partitionMetas.get(partitionMetas.size() - 1);
        return encodeLastComponent(rawPartitionComponentsIN, lastPartitionComponentMeta);
    }

    List<Object> encodeClusteringKeys(List<Object> rawClusteringKeys) {
        log.trace("Encode {} to CQL clustering keys", rawClusteringKeys);
        Validator.validateTrue(meta.type() == PropertyType.EMBEDDED_ID, "Cannot encode clustering components '%s' for the property '%s' which is not a compound primary key", rawClusteringKeys, meta.propertyName);
        final List<PropertyMeta> clusteringMetas = meta.getEmbeddedIdProperties().getClusteringComponents().propertyMetas;
        Validator.validateTrue(rawClusteringKeys.size() <= clusteringMetas.size(),"There should be no more than '%s' clustering components to be encoded for class '%s'", rawClusteringKeys, meta.getEntityClassName());
        return encodeElements(rawClusteringKeys, clusteringMetas);
    }

    List<Object> encodeClusteringKeysIN(List<Object> rawClusteringKeysIN) {
        log.trace("Encode {} to CQL clustering keys IN", rawClusteringKeysIN);
        Validator.validateTrue(meta.type() == PropertyType.EMBEDDED_ID, "Cannot encode clustering components '%s' for the property '%s' which is not a compound primary key", rawClusteringKeysIN, meta.propertyName);
        final List<PropertyMeta> clusteringMetas = meta.getEmbeddedIdProperties().getClusteringComponents().propertyMetas;
        final PropertyMeta lastClusteringKeyMeta = clusteringMetas.get(clusteringMetas.size() - 1);
        return encodeLastComponent(rawClusteringKeysIN, lastClusteringKeyMeta);
    }

    private List<Object> encodeElements(List<Object> rawPartitionComponents, List<PropertyMeta> propertyMetas) {
        List<Object> encoded = new ArrayList<>();
        for (int i = 0; i < rawPartitionComponents.size(); i++) {
            final PropertyMeta componentMeta = propertyMetas.get(i);
            encoded.add(componentMeta.forTranscoding().encodeToCassandra(rawPartitionComponents.get(i)));
        }
        return encoded;
    }

    private List<Object> encodeLastComponent(List<Object> rawPartitionComponentsIN, PropertyMeta lastComponentMeta) {
        List<Object> encoded = new ArrayList<>();
        for (Object rawPartitionComponentIN : rawPartitionComponentsIN) {
            encoded.add(lastComponentMeta.forTranscoding().encodeToCassandra(rawPartitionComponentIN));
        }
        return encoded;
    }

    public String forceEncodeToJSONForCounter(Object object) {
        if (log.isTraceEnabled()) {
            log.trace("Force encode {} to JSON for property {} of entity class {}", object, meta.getPropertyName(), meta.getEntityClassName());
        }
        Validator.validateNotNull(object, "Cannot encode to JSON null primary key for class '%s'", meta.getEntityClassName());
        if (object instanceof String) {
            return String.class.cast(object);
        } else {
            try {
                return this.meta.defaultJacksonMapperForCounter.writeValueAsString(object);
            } catch (Exception e) {
                throw new AchillesException(String.format("Error while encoding primary key '%s' for class '%s'", object, meta.getEntityClassName()), e);
            }
        }
    }
}
