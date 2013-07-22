package info.archinnov.achilles.entity.metadata.transcoding;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * SimpleTranscoder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SimpleTranscoder extends AbstractTranscoder {

    protected ReflectionInvoker invoker = new ReflectionInvoker();

    public SimpleTranscoder(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public Object encode(PropertyMeta<?, ?> pm, Object entityValue) {
        return super.encode(pm, pm.getValueClass(), entityValue);
    }

    @Override
    public Object decode(PropertyMeta<?, ?> pm, Object cassandraValue) {
        return super.decode(pm, pm.getValueClass(), cassandraValue);
    }

}
