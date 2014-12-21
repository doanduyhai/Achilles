package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;

public class PropertyMetaSliceQueryContext extends PropertyMetaView {

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaSliceQueryContext.class);
    private ReflectionInvoker invoker = ReflectionInvoker.Singleton.INSTANCE.get();

    protected PropertyMetaSliceQueryContext(PropertyMeta meta) {
        super(meta);
    }

    public Object instantiateEmbeddedIdWithPartitionComponents(List<Object> partitionComponents) {
        log.trace("Instantiate @CompoundPrimaryKey class {} with partition key components {}", meta.getValueClass().getCanonicalName(), partitionComponents);
        Object newPrimaryKey = meta.forValues().instantiate();
        List<Field> fields = meta.getCompoundPKProperties().getPartitionComponents().getComponentFields();
        for (int i = 0; i < partitionComponents.size(); i++) {
            Field field = fields.get(i);
            Object component = partitionComponents.get(i);
            invoker.setValueToField(newPrimaryKey, field, component);
        }
        return newPrimaryKey;
    }
}
