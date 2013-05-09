package info.archinnov.achilles.iterator.factory;

import static info.archinnov.achilles.helper.LoggerHelper.format;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.JoinEntityHelper;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.KeyValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

/**
 * KeyValueFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueFactory {
    private static final Logger log = LoggerFactory.getLogger(KeyValueFactory.class);

    private JoinEntityHelper joinHelper = new JoinEntityHelper();
    private EntityProxifier proxifier = new EntityProxifier();
    private CompositeTransformer compositeTransformer = new CompositeTransformer();

    public <ID, K, V> KeyValue<K, V> createKeyValue(PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
            HColumn<Composite, ?> hColumn) {
        log.trace("Build key/value for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return compositeTransformer.buildKeyValue(context, propertyMeta, hColumn);
    }

    public <K, V> K createKey(PropertyMeta<K, V> propertyMeta, HColumn<Composite, ?> hColumn) {
        log.trace("Build key for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return compositeTransformer.buildKey(propertyMeta, hColumn);
    }

    public <ID, K, V> V createValue(PersistenceContext<ID> context, PropertyMeta<K, V> propertyMeta,
            HColumn<Composite, ?> hColumn) {
        log.trace("Build key value for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return compositeTransformer.buildValue(context, propertyMeta, hColumn);
    }

    public Integer createTtl(HColumn<Composite, ?> hColumn) {
        log.debug("Build ttl from Hcolumn {}", format(hColumn.getName()));
        return hColumn.getTtl();
    }

    public <K, V, W> List<V> createValueList(PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, W>> hColumns) {
        log.trace("Build value list for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return Lists.transform(hColumns, compositeTransformer.buildValueTransformer(propertyMeta));
    }

    public <K, V, W> List<K> createKeyList(PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, W>> hColumns) {
        log.trace("Build key list for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return Lists.transform(hColumns, compositeTransformer.buildKeyTransformer(propertyMeta));
    }

    public <ID, K, V, W> List<KeyValue<K, V>> createKeyValueList(PersistenceContext<ID> context,
            PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, W>> hColumns) {
        log.trace("Build key/value list for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return Lists.transform(hColumns, compositeTransformer.buildKeyValueTransformer(context, propertyMeta));
    }

    @SuppressWarnings("unchecked")
    public <ID, JOIN_ID, K, V, W> List<V> createJoinValueList(PersistenceContext<ID> context,
            PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, W>> hColumns) {
        log.trace("Build join value list for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());

        List<V> result = new ArrayList<V>();

        if (CollectionUtils.isNotEmpty(hColumns)) {

            EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();

            List<JOIN_ID> joinIds = (List<JOIN_ID>) Lists.transform(hColumns,
                    compositeTransformer.buildRawValueTransformer());

            Map<JOIN_ID, V> joinEntities = loadJoinEntities(context, propertyMeta, joinMeta, joinIds);

            for (JOIN_ID joinId : joinIds) {
                V proxy = buildProxy(context, joinMeta, joinEntities, joinId);
                result.add(proxy);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public <ID, JOIN_ID, K, V, W> List<KeyValue<K, V>> createJoinKeyValueList(PersistenceContext<ID> context,
            PropertyMeta<K, V> propertyMeta, List<HColumn<Composite, W>> hColumns) {
        log.trace("Build join key/value list for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());

        List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();

        if (CollectionUtils.isNotEmpty(hColumns)) {

            EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
            List<K> keys = Lists.transform(hColumns, compositeTransformer.buildKeyTransformer(propertyMeta));
            List<JOIN_ID> joinIds = (List<JOIN_ID>) Lists.transform(hColumns,
                    compositeTransformer.buildRawValueTransformer());

            Map<JOIN_ID, V> joinEntities = loadJoinEntities(context, propertyMeta, joinMeta, joinIds);

            List<Integer> ttls = Lists.transform(hColumns, compositeTransformer.buildTtlTransformer());

            for (int i = 0; i < keys.size(); i++) {
                V proxy = buildProxy(context, joinMeta, joinEntities, joinIds.get(i));
                result.add(new KeyValue<K, V>(keys.get(i), proxy, ttls.get(i)));
            }
        }
        return result;
    }

    // Counter
    public <ID, K> KeyValue<K, Counter> createCounterKeyValue(PersistenceContext<ID> context,
            PropertyMeta<K, Counter> propertyMeta, HCounterColumn<Composite> hColumn) {
        log.trace("Build counter key/value for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return compositeTransformer.buildCounterKeyValue(context, propertyMeta, hColumn);
    }

    public <K> K createCounterKey(PropertyMeta<K, Counter> propertyMeta, HCounterColumn<Composite> hColumn) {
        log.trace("Build counter key for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return compositeTransformer.buildCounterKey(propertyMeta, hColumn);
    }

    public <ID, K> Counter createCounterValue(PersistenceContext<ID> context, PropertyMeta<K, Counter> propertyMeta,
            HCounterColumn<Composite> hColumn) {
        log.trace("Build counter value for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return compositeTransformer.buildCounterValue(context, propertyMeta, hColumn);
    }

    public <ID, K> List<KeyValue<K, Counter>> createCounterKeyValueList(PersistenceContext<ID> context,
            PropertyMeta<K, Counter> propertyMeta, List<HCounterColumn<Composite>> hColumns) {
        log.trace("Build counter key/value list for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return Lists.transform(hColumns, compositeTransformer.buildCounterKeyValueTransformer(context, propertyMeta));
    }

    public <ID, K> List<Counter> createCounterValueList(PersistenceContext<ID> context,
            PropertyMeta<K, Counter> propertyMeta, List<HCounterColumn<Composite>> hColumns) {
        log.trace("Build counter value lsit for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return Lists.transform(hColumns, compositeTransformer.buildCounterValueTransformer(context, propertyMeta));
    }

    public <K> List<K> createCounterKeyList(PropertyMeta<K, Counter> propertyMeta,
            List<HCounterColumn<Composite>> hColumns) {
        log.trace("Build counter key list for property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        return Lists.transform(hColumns, compositeTransformer.buildCounterKeyTransformer(propertyMeta));
    }

    @SuppressWarnings("unchecked")
    private <JOIN_ID, V, ID, K> Map<JOIN_ID, V> loadJoinEntities(PersistenceContext<ID> context,
            PropertyMeta<K, V> propertyMeta, EntityMeta<JOIN_ID> joinMeta, List<JOIN_ID> joinIds) {
        GenericEntityDao<JOIN_ID> joinEntityDao = (GenericEntityDao<JOIN_ID>) context.findEntityDao(joinMeta
                .getColumnFamilyName());

        Map<JOIN_ID, V> joinEntities = joinHelper.loadJoinEntities(propertyMeta.getValueClass(), joinIds, joinMeta,
                joinEntityDao);
        return joinEntities;
    }

    private <V, JOIN_ID, ID> V buildProxy(PersistenceContext<ID> context, EntityMeta<JOIN_ID> joinMeta,
            Map<JOIN_ID, V> joinEntities, Object joinId) {
        V joinEntity = joinEntities.get(joinId);
        PersistenceContext<JOIN_ID> joinContext = context.newPersistenceContext(joinMeta, joinEntity);
        V proxy = proxifier.buildProxy(joinEntity, joinContext);
        return proxy;
    }
}
