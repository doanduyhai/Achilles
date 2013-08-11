package info.archinnov.achilles.entity.metadata.transcoding;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import java.util.HashSet;
import java.util.Set;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * SetTranscoder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SetTranscoder extends AbstractTranscoder {

    public SetTranscoder(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public Set<Object> encode(PropertyMeta pm, Set<?> entityValue) {
        Set<Object> encoded = new HashSet<Object>();
        for (Object value : entityValue)
        {
            encoded.add(super.encode(pm, pm.getValueClass(), value));
        }
        return encoded;
    }

    @Override
    public Set<Object> decode(PropertyMeta pm, Set<?> cassandraValue) {
        Set<Object> decoded = new HashSet<Object>();
        for (Object value : cassandraValue)
        {
            decoded.add(super.decode(pm, pm.getValueClass(), value));
        }
        return decoded;
    }

}
