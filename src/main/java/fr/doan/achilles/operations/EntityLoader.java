package fr.doan.achilles.operations;

import java.io.Serializable;
import java.util.List;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.cassandra.utils.Pair;
import fr.doan.achilles.bean.BeanMapper;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.validation.Validator;

public class EntityLoader {

    private BeanMapper mapper = new BeanMapper();

    public <T extends Object, ID extends Serializable> T load(Class<T> entityClass, ID key, EntityMeta<ID> entityMeta) {
        Validator.validateNotNull(entityClass, "entity class");
        Validator.validateNotNull(key, "entity key");
        Validator.validateNotNull(entityMeta, "entity meta");

        T entity = null;
        try {

            List<Pair<Composite, Object>> columns = entityMeta.getDao().eagerFetchEntity(key);

            if (columns.size() > 0) {
                entity = entityClass.newInstance();
                mapper.mapColumnsToBean(key, columns, entityMeta, entity);
            }

        } catch (Exception e) {
            throw new RuntimeException("Error when loading entity type '" + entityClass.getCanonicalName()
                    + "' with key '" + key + "'", e);
        }
        return entity;
    }

}
