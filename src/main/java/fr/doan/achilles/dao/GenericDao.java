package fr.doan.achilles.dao;

import static fr.doan.achilles.serializer.Utils.BYTE_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.validation.Validator;

public class GenericDao<K> extends AbstractDao<K, Composite, Object> {

    private Serializer<K> keySerializer;

    protected GenericDao() {
    }

    public GenericDao(Keyspace keyspace, Serializer<K> keySerializer, String columnFamily) {
        super(keyspace);
        Validator.validateNotBlank(columnFamily, "columnFamily");
        Validator.validateNotNull(keySerializer, "keySerializer for columnFamily ='" + columnFamily + "'");
        this.keySerializer = keySerializer;
        this.columnFamily = columnFamily;
    }

    public Mutator<K> buildMutator() {
        return HFactory.createMutator(this.keyspace, this.keySerializer);
    }

    public Composite buildCompositeForProperty(String propertyName, PropertyType type, int hashOrPosition) {
        Composite composite = new Composite();
        composite.addComponent(type.flag(), BYTE_SRZ);
        composite.addComponent(propertyName, STRING_SRZ);
        composite.addComponent(hashOrPosition, INT_SRZ);
        return composite;
    }
}
