package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.type.InsertStrategy;

import java.util.ArrayList;
import java.util.List;

public class EntityMetaOperations extends EntityMetaView {

    protected ReflectionInvoker invoker = new ReflectionInvoker();

    protected EntityMetaOperations(EntityMeta meta) {
        super(meta);
    }

    public Object getPrimaryKey(Object entity) {
        return meta.getIdMeta().forValues().getPrimaryKey(entity);
    }

    @SuppressWarnings("unchecked")
    public <T> T instanciate() {
        return (T) invoker.instantiate(meta.getEntityClass());
    }


    public List<PropertyMeta> getColumnsMetaToLoad() {
        if (meta.structure().isClusteredCounter()) {
            return new ArrayList<>(meta.getPropertyMetas().values());
        } else {
            return meta.getAllMetasExceptCounters();
        }
    }

    public List<PropertyMeta> retrievePropertyMetasForInsert(Object entity) {
        if (meta.config().getInsertStrategy() == InsertStrategy.ALL_FIELDS) {
            return meta.getAllMetasExceptIdAndCounters();
        }

        List<PropertyMeta> metasForNonNullProperties = new ArrayList<>();
        for (PropertyMeta propertyMeta : meta.getAllMetasExceptIdAndCounters()) {
            if (propertyMeta.forValues().getValueFromField(entity) != null) {
                metasForNonNullProperties.add(propertyMeta);
            }
        }
        return metasForNonNullProperties;
    }
}
