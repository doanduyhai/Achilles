package fr.doan.achilles.iterator;

import java.util.NoSuchElementException;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;

/**
 * KeyValueIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueIteratorForWideRow<K, V> implements KeyValueIterator<K, V> {
    protected ColumnSliceIterator<?, Composite, Object> columnSliceIterator;
    private KeyValueFactory factory = new KeyValueFactory();
    private PropertyMeta<K, V> wideMapMeta;

    protected KeyValueIteratorForWideRow() {
    }

    public KeyValueIteratorForWideRow(ColumnSliceIterator<?, Composite, Object> columnSliceIterator,
            PropertyMeta<K, V> wideMapMeta) {
        this.columnSliceIterator = columnSliceIterator;
        this.wideMapMeta = wideMapMeta;
    }

    @Override
    public boolean hasNext() {
        return this.columnSliceIterator.hasNext();
    }

    @Override
    public KeyValue<K, V> next() {
        KeyValue<K, V> keyValue = null;
        if (this.columnSliceIterator.hasNext()) {
            HColumn<Composite, Object> column = this.columnSliceIterator.next();
            V value = wideMapMeta.getValue(column.getValue());
            K key = wideMapMeta.getKey(column.getName().get(0, wideMapMeta.getKeySerializer()));
            keyValue = factory.create(key, value, column.getTtl());
        } else {
            throw new NoSuchElementException();
        }
        return keyValue;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(
                "Remove from iterator is not supported. Please use removeValue() or removeValues() instead");
    }

}
