package info.archinnov.achilles.entity.metadata.transcoding;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * MapTranscoder
 * 
 * @author DuyHai DOAN
 * 
 */
public class MapTranscoder extends AbstractTranscoder {

    public MapTranscoder(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public Object encode(PropertyMeta<?, ?> pm, Object entityValue) {
        return super.encode(pm, pm.getValueClass(), entityValue);
    }

    @Override
    public Object encodeKey(PropertyMeta<?, ?> pm, Object entityValue) {
        return super.encodeIgnoreJoin(pm.getKeyClass(), entityValue);
    }

    @Override
    public Map<Object, Object> encode(PropertyMeta<?, ?> pm, Map<?, ?> entityValue) {
        Map<Object, Object> encoded = new HashMap<Object, Object>();
        for (Entry<?, ?> entry : entityValue.entrySet())
        {
            Object encodedKey = super.encodeIgnoreJoin(pm.getKeyClass(), entry.getKey());
            Object encodedValue = super.encode(pm, pm.getValueClass(), entry.getValue());
            encoded.put(encodedKey, encodedValue);
        }
        return encoded;
    }

    @Override
    public Object decode(PropertyMeta<?, ?> pm, Object cassandraValue) {
        return super.decode(pm, pm.getValueClass(), cassandraValue);
    }

    @Override
    public Object decodeKey(PropertyMeta<?, ?> pm, Object cassandraValue) {
        return super.decodeIgnoreJoin(pm.getKeyClass(), cassandraValue);
    }

    @Override
    public Map<Object, Object> decode(PropertyMeta<?, ?> pm, Map<?, ?> cassandraValue) {
        Map<Object, Object> decoded = new HashMap<Object, Object>();
        for (Entry<?, ?> entry : cassandraValue.entrySet())
        {
            Object decodedKey = super.decodeIgnoreJoin(pm.getKeyClass(), entry.getKey());
            Object decodedValue = super.decode(pm, pm.getValueClass(), entry.getValue());
            decoded.put(decodedKey, decodedValue);
        }
        return decoded;
    }

}
