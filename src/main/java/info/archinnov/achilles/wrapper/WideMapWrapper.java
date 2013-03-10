package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import java.util.List;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * WideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapper<ID, K, V> extends AbstractWideMapWrapper<K, V> {

    protected ID id;
    protected GenericDynamicCompositeDao<ID> entityDao;
    protected GenericCompositeDao<ID, ?> columnFamilyDao;
    protected PropertyMeta<K, V> propertyMeta;
    protected EntityHelper entityHelper;

    protected CompositeHelper compositeHelper;
    protected KeyValueFactory keyValueFactory;
    protected IteratorFactory iteratorFactory;
    protected DynamicCompositeKeyFactory keyFactory;

    protected DynamicComposite buildComposite(K key) {
        return keyFactory.createForInsert(propertyMeta, key);
    }

    @Override
    public V get(K key) {
        Object value = entityDao.getValue(id, buildComposite(key));

        return propertyMeta.getValueFromString(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void insert(K key, V value, int ttl) {
        if (this.interceptor.isBatchMode()) {
            entityDao.setValueBatch(id, buildComposite(key), propertyMeta.writeValueToString(value), ttl,
                    (Mutator<ID>) interceptor.getMutator());
        } else {
            entityDao.setValue(id, buildComposite(key), propertyMeta.writeValueToString(value), ttl);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void insert(K key, V value) {
        if (this.interceptor.isBatchMode()) {
            entityDao.setValueBatch(id, buildComposite(key), propertyMeta.writeValueToString(value),
                    (Mutator<ID>) interceptor.getMutator());
        } else {
            entityDao.setValue(id, buildComposite(key), propertyMeta.writeValueToString(value));
        }
    }

    @Override
    public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds, OrderingMode ordering) {

        compositeHelper.checkBounds(propertyMeta, start, end, ordering);

        DynamicComposite[] queryComps = keyFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);

        List<HColumn<DynamicComposite, String>> hColumns = entityDao.findRawColumnsRange(id, queryComps[0],
                queryComps[1], count, ordering.reverse());

        if (propertyMeta.isJoin()) {
            return keyValueFactory.createJoinKeyValueListForDynamicComposite(propertyMeta, hColumns);
        } else {

            return keyValueFactory.createKeyValueListForDynamicComposite(propertyMeta, hColumns);
        }
    }

    @Override
    public List<V> findValues(K start, K end, int count, BoundingMode bounds, OrderingMode ordering) {

        compositeHelper.checkBounds(propertyMeta, start, end, ordering);

        DynamicComposite[] queryComps = keyFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);

        List<HColumn<DynamicComposite, String>> hColumns = entityDao.findRawColumnsRange(id, queryComps[0],
                queryComps[1], count, ordering.reverse());
        if (propertyMeta.isJoin()) {
            return keyValueFactory.createJoinValueListForDynamicComposite(propertyMeta, hColumns);
        } else {

            return keyValueFactory.createValueListForDynamicComposite(propertyMeta, hColumns);
        }
    }

    @Override
    public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering) {

        compositeHelper.checkBounds(propertyMeta, start, end, ordering);

        DynamicComposite[] queryComps = keyFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);

        List<HColumn<DynamicComposite, String>> hColumns = entityDao.findRawColumnsRange(id, queryComps[0],
                queryComps[1], count, ordering.reverse());
        return keyValueFactory.createKeyListForDynamicComposite(propertyMeta, hColumns);
    }

    @Override
    public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds, OrderingMode ordering) {

        DynamicComposite[] queryComps = keyFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);

        if (propertyMeta.isJoin()) {

            AchillesJoinSliceIterator<ID, DynamicComposite, String, K, V> joinColumnSliceIterator = entityDao
                    .getJoinColumnsIterator(propertyMeta, id, queryComps[0], queryComps[1], ordering.reverse(), count);

            return iteratorFactory.createKeyValueJoinIteratorForDynamicComposite(joinColumnSliceIterator,
                    propertyMeta);

        } else {

            AchillesSliceIterator<ID, DynamicComposite, String> columnSliceIterator = entityDao.getColumnsIterator(
                    id, queryComps[0], queryComps[1], ordering.reverse(), count);

            return iteratorFactory.createKeyValueIteratorForDynamicComposite(columnSliceIterator, propertyMeta);
        }
    }

    @Override
    public void remove(K key) {
        entityDao.removeColumn(id, buildComposite(key));
    }

    @Override
    public void remove(K start, K end, BoundingMode bounds) {

        compositeHelper.checkBounds(propertyMeta, start, end, OrderingMode.ASCENDING);

        DynamicComposite[] queryComps = keyFactory.createForQuery(//
                propertyMeta, start, end, bounds, OrderingMode.ASCENDING);

        entityDao.removeColumnRange(id, queryComps[0], queryComps[1]);
    }

    @Override
    public void removeFirst(int count) {
        entityDao.removeColumnRange(id, null, null, false, count);

    }

    @Override
    public void removeLast(int count) {
        entityDao.removeColumnRange(id, null, null, true, count);
    }

    public void setId(ID id) {
        this.id = id;
    }

    public void setEntityDao(GenericDynamicCompositeDao<ID> entityDao) {
        this.entityDao = entityDao;
    }

    public void setColumnFamilyDao(GenericCompositeDao<ID, ?> columnFamilyDao) {
        this.columnFamilyDao = columnFamilyDao;
    }

    public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta) {
        this.propertyMeta = wideMapMeta;
    }

    public void setEntityHelper(EntityHelper entityHelper) {
        this.entityHelper = entityHelper;
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

    public void setKeyFactory(DynamicCompositeKeyFactory keyFactory) {
        this.keyFactory = keyFactory;
    }
}
