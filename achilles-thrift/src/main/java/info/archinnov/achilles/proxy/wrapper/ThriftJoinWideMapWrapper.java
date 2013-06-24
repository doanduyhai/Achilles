package info.archinnov.achilles.proxy.wrapper;

import static info.archinnov.achilles.logger.ThriftLoggerHelper.format;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.iterator.ThriftJoinSliceIterator;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;
import java.util.List;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;

/**
 * ThriftJoinWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftJoinWideMapWrapper<K, V> extends ThriftAbstractWideMapWrapper<K, V>
{

    private static final Logger log = LoggerFactory.getLogger(ThriftJoinWideMapWrapper.class);

    private Object id;
    private PropertyMeta<K, V> propertyMeta;
    private ThriftGenericWideRowDao dao;
    private ThriftEntityPersister persister = new ThriftEntityPersister();
    private ThriftEntityLoader loader = new ThriftEntityLoader();
    private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();

    private Composite buildComposite(K key)
    {
        return thriftCompositeFactory.createBaseComposite(propertyMeta, key);
    }

    @Override
    public V get(K key)
    {
        log.trace("Get join value having key {}", key);
        Validator.validateNotNull(key, "Key should be provided to insert data into WideMap");

        V result = null;
        Object joinId = dao.getValue(id, buildComposite(key));
        if (joinId != null)
        {
            EntityMeta joinMeta = propertyMeta.joinMeta();

            ThriftPersistenceContext joinContext = context.createContextForJoin(
                    propertyMeta.getValueClass(), joinMeta, joinId);

            result = loader.<V> load(joinContext, propertyMeta.getValueClass());
            result = proxifier.buildProxy(result, joinContext);
        }
        return result;
    }

    @Override
    public void insert(K key, V value, int ttl)
    {
        log.trace("Insert join value {} with key {} and ttl {}", value, key, ttl);
        Validator.validateNotNull(key, "Key should be provided to insert data into WideMap");
        Validator.validateNotNull(value, "Value should be provided to insert data into WideMap");

        Object joinId = persistOrEnsureJoinEntityExists(value);
        dao.setValueBatch(id, buildComposite(key), joinId, Optional.fromNullable(ttl),
                context.getWideRowMutator(getExternalCFName()));
        context.flush();
    }

    @Override
    public void insert(K key, V value)
    {
        log.trace("Insert join value {} with key {}", value, key);
        Validator.validateNotNull(key, "Key should be provided to insert data into WideMap");
        Validator.validateNotNull(value, "Value should be provided to insert data into WideMap");

        Object joinId = persistOrEnsureJoinEntityExists(value);
        dao.setValueBatch(id, buildComposite(key), joinId, Optional.<Integer> absent(),
                context.getWideRowMutator(getExternalCFName()));
        context.flush();
    }

    @Override
    public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds,
            OrderingMode ordering)
    {

        queryValidator.validateBoundsForQuery(propertyMeta, start, end, ordering);

        Composite[] queryComps = thriftCompositeFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);

        if (log.isTraceEnabled())
        {
            log.trace(
                    "Find key/join value pairs in range {} / {} with bounding {} and ordering {}",
                    format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
        }

        List<HColumn<Composite, Object>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
                queryComps[1], count, ordering.isReverse());

        return thriftKeyValueFactory.createJoinKeyValueList(context, propertyMeta, hColumns);
    }

    @Override
    public List<V> findValues(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
    {
        queryValidator.validateBoundsForQuery(propertyMeta, start, end, ordering);

        Composite[] queryComps = thriftCompositeFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);
        if (log.isTraceEnabled())
        {
            log.trace("Find join values in range {} / {} with bounding {} and ordering {}",
                    format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
        }
        List<HColumn<Composite, Object>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
                queryComps[1], count, ordering.isReverse());

        return thriftKeyValueFactory.createJoinValueList(context, propertyMeta, hColumns);
    }

    @Override
    public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
    {
        queryValidator.validateBoundsForQuery(propertyMeta, start, end, ordering);

        Composite[] queryComps = thriftCompositeFactory.createForQuery( //
                propertyMeta, start, end, bounds, ordering);

        if (log.isTraceEnabled())
        {
            log.trace("Find keys in range {} / {} with bounding {} and ordering {}",
                    format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
        }

        List<HColumn<Composite, Object>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
                queryComps[1], count, ordering.isReverse());

        return thriftKeyValueFactory.createKeyList(propertyMeta, hColumns);
    }

    @Override
    public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds,
            OrderingMode ordering)
    {
        Composite[] composites = thriftCompositeFactory.createForQuery(propertyMeta, start, end,
                bounds, ordering);

        if (log.isTraceEnabled())
        {
            log
                    .trace("Iterate in range {} / {} with bounding {} and ordering {} and batch of {} elements",
                            format(composites[0]), format(composites[1]), bounds.name(),
                            ordering.name(), count);
        }

        ThriftGenericEntityDao joinEntityDao = context.findEntityDao(propertyMeta
                .joinMeta()
                .getTableName());

        ThriftJoinSliceIterator<?, K, V> joinColumnSliceIterator = dao.getJoinColumnsIterator(
                joinEntityDao, propertyMeta, id, composites[0], composites[1],
                ordering.isReverse(), count);

        return thriftIteratorFactory.createJoinKeyValueIterator(context, joinColumnSliceIterator,
                propertyMeta);
    }

    @Override
    public void remove(K key)
    {
        log.trace("Remove join value having key {}", key);

        dao.removeColumnBatch(id, buildComposite(key),
                context.getWideRowMutator(getExternalCFName()));
        context.flush();
    }

    @Override
    public void remove(K start, K end, BoundingMode bounds)
    {
        queryValidator.validateBoundsForQuery(propertyMeta, start, end,
                OrderingMode.ASCENDING);

        Composite[] queryComps = thriftCompositeFactory.createForQuery(//
                propertyMeta, start, end, bounds, OrderingMode.ASCENDING);
        if (log.isTraceEnabled())
        {
            log.trace("Remove join values in range {} / {} with bounding {} and ordering {}",
                    format(queryComps[0]), format(queryComps[1]), bounds.name(),
                    OrderingMode.ASCENDING.name());
        }
        dao.removeColumnRangeBatch(id, queryComps[0], queryComps[1],
                context.getWideRowMutator(getExternalCFName()));
        context.flush();
    }

    @Override
    public void removeFirst(int count)
    {
        log.trace("Remove first {} join values", count);
        dao.removeColumnRangeBatch(id, null, null, false, count,
                context.getWideRowMutator(getExternalCFName()));
        context.flush();
    }

    @Override
    public void removeLast(int count)
    {
        log.trace("Remove last {} join values", count);
        dao.removeColumnRangeBatch(id, null, null, true, count,
                context.getWideRowMutator(getExternalCFName()));
        context.flush();
    }

    private Object persistOrEnsureJoinEntityExists(V value)
    {
        Object joinId = null;
        JoinProperties joinProperties = propertyMeta.getJoinProperties();

        if (value != null)
        {
            ThriftPersistenceContext joinContext = context
                    .createContextForJoin(propertyMeta.joinMeta(), value);

            joinId = persister.cascadePersistOrEnsureExists(joinContext, value, joinProperties);
        }
        else
        {
            throw new IllegalArgumentException("Cannot persist null entity");
        }
        return joinId;
    }

    private String getExternalCFName()
    {
        return propertyMeta.getExternalTableName();
    }

    public void setId(Object id)
    {
        this.id = id;
    }

    public void setExternalWideMapMeta(PropertyMeta<K, V> externalWideMapMeta)
    {
        this.propertyMeta = externalWideMapMeta;
    }

    public void setDao(ThriftGenericWideRowDao dao)
    {
        this.dao = dao;
    }
}
