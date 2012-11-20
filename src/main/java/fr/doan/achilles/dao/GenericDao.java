package fr.doan.achilles.dao;

import static fr.doan.achilles.metadata.PropertyType.END_EAGER;
import static fr.doan.achilles.metadata.PropertyType.START_EAGER;
import static fr.doan.achilles.serializer.Utils.BYTE_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import java.util.List;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.cassandra.utils.Pair;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.validation.Validator;

public class GenericDao<K> extends AbstractDao<K, Composite, Object> {

    private Serializer<K> keySerializer;
    private Composite startCompositeForEagerFetch;
    private Composite endCompositeForEagerFetch;

    protected GenericDao() {
        initComposites();
    }

    public GenericDao(Keyspace keyspace, Serializer<K> keySerializer, String columnFamily) {
        super(keyspace);
        initComposites();
        Validator.validateNotBlank(columnFamily, "columnFamily");
        Validator.validateNotNull(keySerializer, "keySerializer for columnFamily ='" + columnFamily + "'");
        this.keySerializer = keySerializer;
        this.columnFamily = columnFamily;
    }

    private void initComposites() {
        startCompositeForEagerFetch = new Composite();
        startCompositeForEagerFetch.addComponent(0, START_EAGER.flag(), ComponentEquality.EQUAL);

        endCompositeForEagerFetch = new Composite();
        endCompositeForEagerFetch.addComponent(0, END_EAGER.flag(), ComponentEquality.GREATER_THAN_EQUAL);
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

    public List<Pair<Composite, Object>> eagerFetchEntity(K key) {

        return this.findColumnsRange(key, startCompositeForEagerFetch, endCompositeForEagerFetch, false,
                Integer.MAX_VALUE);
    }
}
