package info.archinnov.achilles.entity.metadata.transcoding;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * ListTranscoder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListTranscoder extends AbstractTranscoder {

    public ListTranscoder(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public List<Object> encode(PropertyMeta<?, ?> pm, List<?> entityValue) {
        List<Object> encoded = new ArrayList<Object>();
        for (Object value : entityValue)
        {
            encoded.add(super.encode(pm, pm.getValueClass(), value));
        }
        return encoded;
    }

    @Override
    public List<Object> decode(PropertyMeta<?, ?> pm, List<?> cassandraValue) {
        List<Object> decoded = new ArrayList<Object>();
        for (Object value : cassandraValue)
        {
            decoded.add(super.decode(pm, pm.getValueClass(), value));
        }
        return decoded;
    }
}
