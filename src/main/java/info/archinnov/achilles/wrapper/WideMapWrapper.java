package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import java.util.List;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * ExternalWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapper<ID, K, V> extends AbstractWideMapWrapper<ID, K, V> {
    protected ID id;
    protected GenericColumnFamilyDao<ID, V> dao;
    protected PropertyMeta<K, V> propertyMeta;
    private CompositeHelper compositeHelper;
    private KeyValueFactory keyValueFactory;
    private IteratorFactory iteratorFactory;
    private CompositeFactory compositeFactory;

    protected Composite buildComposite(K key) {
        Composite comp = compositeFactory.createBaseComposite(propertyMeta, key);
        return comp;
    }

    @Override
    public V get(K key) {
        V result = null;
        Object value = dao.getValue(id, buildComposite(key));
        if (value != null) {
            result = propertyMeta.castValue(value);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void insert(K key, V value) {
        dao.setValueBatch(id, buildComposite(key), (V) propertyMeta.writeValueAsSupportedTypeOrString(value),
                context.getColumnFamilyMutator(getExternalCFName()));
        context.flush();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void insert(K key, V value, int ttl) {
        dao.setValueBatch(id, buildComposite(key), (V) propertyMeta.writeValueAsSupportedTypeOrString(value), ttl,
                context.getColumnFamilyMutator(getExternalCFName()));
        context.flush();
    }

    @Override
    public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds, OrderingMode ordering) {
        compositeHelper.checkBounds(propertyMeta, start, end, ordering);

        Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds, ordering);

        List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0], composites[1], count,
                ordering.isReverse());

        return keyValueFactory.createKeyValueList(context, propertyMeta, hColumns);
    }

    @Override
    public List<V> findValues(K start, K end, int count, BoundingMode bounds, OrderingMode ordering) {
        compositeHelper.checkBounds(propertyMeta, start, end, ordering);

        Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds, ordering);

        List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0], composites[1], count,
                ordering.isReverse());

        return keyValueFactory.createValueList(propertyMeta, hColumns);
    }

    @Override
    public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering) {
        compositeHelper.checkBounds(propertyMeta, start, end, ordering);

        Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds, ordering);

        List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0], composites[1], count,
                ordering.isReverse());

        return keyValueFactory.createKeyList(propertyMeta, hColumns);
    }

    @Override
    public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds, OrderingMode ordering) {

        Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds, ordering);

        AchillesSliceIterator<ID, V> columnSliceIterator = dao.getColumnsIterator(id, composites[0], composites[1],
                ordering.isReverse(), count);

        return iteratorFactory.createKeyValueIterator(context, columnSliceIterator, propertyMeta);

    }

    @Override
    public void remove(K key) {
        dao.removeColumnBatch(id, buildComposite(key), context.getColumnFamilyMutator(getExternalCFName()));
        context.flush();
    }

    @Override
    public void remove(K start, K end, BoundingMode bounds) {
        compositeHelper.checkBounds(propertyMeta, start, end, OrderingMode.ASCENDING);
        Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds,
                OrderingMode.ASCENDING);
        dao.removeColumnRangeBatch(id, composites[0], composites[1],
                context.getColumnFamilyMutator(getExternalCFName()));
        context.flush();
    }

    @Override
    public void removeFirst(int count) {
        dao.removeColumnRangeBatch(id, null, null, false, count, context.getColumnFamilyMutator(getExternalCFName()));
        context.flush();
    }

    @Override
    public void removeLast(int count) {
        dao.removeColumnRangeBatch(id, null, null, true, count, context.getColumnFamilyMutator(getExternalCFName()));
        context.flush();
    }

    private String getExternalCFName() {
        return propertyMeta.getExternalCFName();
    }

    public void setId(ID id) {
        this.id = id;
    }

    public void setDao(GenericColumnFamilyDao<ID, V> dao) {
        this.dao = dao;
    }

    public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta) {
        this.propertyMeta = wideMapMeta;
    }

    public void setCompositeHelper(CompositeHelper compositeHelper) {
        this.compositeHelper = compositeHelper;
    }

    public void setKeyValueFactory(KeyValueFactory keyValueFactory) {
        this.keyValueFactory = keyValueFactory;
    }

    public void setIteratorFactory(IteratorFactory iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    public void setCompositeKeyFactory(CompositeFactory compositeFactory) {
        this.compositeFactory = compositeFactory;
    }
}
