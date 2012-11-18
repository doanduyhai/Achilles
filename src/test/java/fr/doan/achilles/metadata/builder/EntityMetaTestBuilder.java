package fr.doan.achilles.metadata.builder;

import java.io.Serializable;
import me.prettyprint.hector.api.Keyspace;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.parser.EntityParser;

public class EntityMetaTestBuilder {

    private EntityParser parser = new EntityParser();

    public static EntityMetaTestBuilder entityMeta() {
        return new EntityMetaTestBuilder();
    }

    @SuppressWarnings("unchecked")
    public <ID extends Serializable> EntityMeta<ID> build(Keyspace keyspace, GenericDao<ID> dao, Class<?> entityClass) {
        EntityMeta<ID> entityMeta = (EntityMeta<ID>) parser.parseEntity(keyspace, entityClass);
        entityMeta.setDao(dao);

        return entityMeta;
    }
}
