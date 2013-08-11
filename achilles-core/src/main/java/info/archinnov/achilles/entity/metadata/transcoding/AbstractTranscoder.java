package info.archinnov.achilles.entity.metadata.transcoding;

import static info.archinnov.achilles.helper.PropertyHelper.isSupportedType;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * AbstractTranscoder
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AbstractTranscoder implements DataTranscoder {

    protected ObjectMapper objectMapper;
    protected ReflectionInvoker invoker = new ReflectionInvoker();

    public AbstractTranscoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Object encode(PropertyMeta pm, Object entityValue) {
        throw new AchillesException("Transcoder cannot encode value '" + entityValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public Object encodeKey(PropertyMeta pm, Object entityValue) {
        throw new AchillesException("Transcoder cannot encode key '" + entityValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public List<Object> encode(PropertyMeta pm, List<?> entityValue) {
        throw new AchillesException("Transcoder cannot encode value '" + entityValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public Set<Object> encode(PropertyMeta pm, Set<?> entityValue) {
        throw new AchillesException("Transcoder cannot encode value '" + entityValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public Map<Object, Object> encode(PropertyMeta pm, Map<?, ?> entityValue) {
        throw new AchillesException("Transcoder cannot encode value '" + entityValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public List<Object> encodeToComponents(PropertyMeta pm, Object compoundKey)
    {
        throw new AchillesException("Transcoder cannot encode from value '" + compoundKey
                + "' to components for type '" + pm.type().name() + "'");
    }

    @Override
    public List<Object> encodeComponents(PropertyMeta pm, List<?> components)
    {
        throw new AchillesException("Transcoder cannot encode components value '" + components
                + "'");
    }

    @Override
    public Object decode(PropertyMeta pm, Object cassandraValue) {
        throw new AchillesException("Transcoder cannot decode value '" + cassandraValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public Object decodeKey(PropertyMeta pm, Object cassandraValue) {
        throw new AchillesException("Transcoder cannot decode key '" + cassandraValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public List<Object> decode(PropertyMeta pm, List<?> cassandraValue) {
        throw new AchillesException("Transcoder cannot decode value '" + cassandraValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public Set<Object> decode(PropertyMeta pm, Set<?> cassandraValue) {
        throw new AchillesException("Transcoder cannot decode value '" + cassandraValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public Map<Object, Object> decode(PropertyMeta pm, Map<?, ?> cassandraValue) {
        throw new AchillesException("Transcoder cannot decode value '" + cassandraValue
                + "' for type '" + pm.type().name() + "'");
    }

    @Override
    public Object decodeFromComponents(PropertyMeta pm, List<?> components)
    {
        throw new AchillesException("Transcoder cannot decode from components '" + components
                + "' to value for type '" + pm.type().name() + "'");
    }

    protected Object encode(PropertyMeta pm, Class<?> sourceType, Object entityValue)
    {
        if (pm.type().isJoin()) {
            PropertyMeta joinIdMeta = pm.joinIdMeta();
            Object joinId = invoker.getPrimaryKey(entityValue, joinIdMeta);
            return joinIdMeta.encode(joinId);
        }
        else {
            return encodeIgnoreJoin(sourceType, entityValue);
        }
    }

    protected Object encodeIgnoreJoin(Class<?> sourceType, Object entityValue) {
        if (isSupportedType(sourceType)) {
            return entityValue;
        } else if (sourceType.isEnum()) {
            return ((Enum) entityValue).name();
        } else {
            return forceEncodeToJSON(entityValue);
        }
    }

    protected Object decode(PropertyMeta pm, Class<?> targetType, Object cassandraValue)
    {
        if (pm.type().isJoin()) {
            return pm.joinIdMeta().decode(cassandraValue);
        } else {
            return decodeIgnoreJoin(targetType, cassandraValue);
        }

    }

    protected Object decodeIgnoreJoin(Class<?> targetType, Object cassandraValue) {
        if (isSupportedType(targetType)) {
            return cassandraValue;
        } else if (targetType.isEnum()) {
            return Enum.valueOf((Class) targetType, (String) cassandraValue);
        } else if (cassandraValue instanceof String) {
            return forceDecodeFromJSON((String) cassandraValue, targetType);
        } else {
            throw new AchillesException("Error while decoding value '" + cassandraValue + "' to type '"
                    + targetType.getCanonicalName() + "'");
        }
    }

    @Override
    public String forceEncodeToJSON(Object object)
    {
        try {
            return this.objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new AchillesException("Error while encoding value '" + object + "'", e);
        }
    }

    @Override
    public Object forceDecodeFromJSON(String cassandraValue, Class<?> targetType)
    {
        try {
            return objectMapper.readValue(cassandraValue, targetType);
        } catch (Exception e) {
            throw new AchillesException("Error while decoding value '" + cassandraValue + "' to type '"
                    + targetType.getCanonicalName() + "'", e);
        }
    }
}
