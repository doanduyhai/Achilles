package info.archinnov.achilles.internal.persistence.operations;

import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

public class CounterLoader {

    private EntityMapper mapper = new EntityMapper();
    private ConsistencyOverrider overrider = new ConsistencyOverrider();

    public <T> T loadClusteredCounters(PersistenceContext context) {
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        T entity = null;
        ConsistencyLevel readLevel = overrider.getReadLevel(context,entityMeta);
        Row row =context.getClusteredCounter(readLevel);
        if(row != null) {
            entity = entityMeta.instanciate();
            entityMeta.getIdMeta().setValueToField(entity, primaryKey);

            for(PropertyMeta counterMeta:context.getAllCountersMeta()) {
                mapper.setCounterToEntity(counterMeta, entity, row);
            }
        }
        return entity;
    }


    public void loadClusteredCounterColumn(PersistenceContext context, Object realObject, PropertyMeta counterMeta) {
        ConsistencyLevel readLevel = overrider.getReadLevel(context,counterMeta);
        Long counterValue = context.getClusteredCounterColumn(counterMeta, readLevel);
        mapper.setCounterToEntity(counterMeta, realObject, counterValue);
    }

    public void loadCounter(PersistenceContext context, Object entity, PropertyMeta counterMeta) {
        ConsistencyLevel readLevel = overrider.getReadLevel(context,counterMeta);
        final Long initialCounterValue = context.getSimpleCounter(counterMeta, readLevel);
        mapper.setCounterToEntity(counterMeta, entity, initialCounterValue);
    }

}
