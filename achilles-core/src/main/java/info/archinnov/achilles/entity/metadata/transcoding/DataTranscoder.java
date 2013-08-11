package info.archinnov.achilles.entity.metadata.transcoding;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transcoder
 * 
 * @author DuyHai DOAN
 * 
 */
public interface DataTranscoder {

    // Encode
    public Object encode(PropertyMeta pm, Object entityValue);

    public Object encodeKey(PropertyMeta pm, Object entityValue);

    public List<Object> encode(PropertyMeta pm, List<?> entityValue);

    public Set<Object> encode(PropertyMeta pm, Set<?> entityValue);

    public Map<Object, Object> encode(PropertyMeta pm, Map<?, ?> entityValue);

    public List<Object> encodeToComponents(PropertyMeta pm, Object compoundKey);

    public String forceEncodeToJSON(Object object);

    //Decode
    public Object decode(PropertyMeta pm, Object cassandraValue);

    public Object decodeKey(PropertyMeta pm, Object cassandraValue);

    public List<Object> decode(PropertyMeta pm, List<?> cassandraValue);

    public Set<Object> decode(PropertyMeta pm, Set<?> cassandraValue);

    public Map<Object, Object> decode(PropertyMeta pm, Map<?, ?> cassandraValue);

    public Object decodeFromComponents(PropertyMeta pm, List<?> components);

    public Object forceDecodeFromJSON(String cassandraValue, Class<?> targetType);
}
