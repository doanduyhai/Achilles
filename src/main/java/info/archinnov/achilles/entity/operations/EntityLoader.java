package info.archinnov.achilles.entity.operations;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.validation.Validator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import org.apache.commons.lang.StringUtils;

/**
 * EntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityLoader {
    private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();
    private EntityMapper mapper = new EntityMapper();
    private EntityHelper helper = new EntityHelper();
    private JoinEntityLoader joinLoader = new JoinEntityLoader();

    public <T, ID> T load(Class<T> entityClass, ID key, EntityMeta<ID> entityMeta) {
        Validator.validateNotNull(entityClass, "Entity class should not be null");
        Validator.validateNotNull(key, "Entity '" + entityClass.getCanonicalName() + "' key should not be null");
        Validator.validateNotNull(entityMeta, "Entity meta for '" + entityClass.getCanonicalName()
                + "' should not be null");

        T entity = null;
        try {

            if (entityMeta.isColumnFamilyDirectMapping()) {
                entity = entityClass.newInstance();
                helper.setValueToField(entity, entityMeta.getIdMeta().getSetter(), key);
            } else {
                List<Pair<DynamicComposite, String>> columns = entityMeta.getEntityDao().eagerFetchEntity(key);
                if (columns.size() > 0) {
                    entity = entityClass.newInstance();
                    mapper.setEagerPropertiesToEntity(key, columns, entityMeta, entity);
                    helper.setValueToField(entity, entityMeta.getIdMeta().getSetter(), key);

                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Error when loading entity type '" + entityClass.getCanonicalName()
                    + "' with key '" + key + "'", e);
        }
        return entity;
    }

    protected <ID, V> Long loadVersionSerialUID(ID key, GenericDynamicCompositeDao<ID> dao) {
        DynamicComposite composite = new DynamicComposite();
        composite.addComponent(0, PropertyType.SERIAL_VERSION_UID.flag(), ComponentEquality.EQUAL);
        composite.addComponent(1, PropertyType.SERIAL_VERSION_UID.name(), ComponentEquality.EQUAL);

        String serialVersionUIDString = dao.getValue(key, composite);
        if (StringUtils.isNotBlank(serialVersionUIDString)) {
            return Long.parseLong(serialVersionUIDString);
        } else {
            return null;
        }
    }

    protected <ID, V> V loadSimpleProperty(ID key, GenericDynamicCompositeDao<ID> dao, PropertyMeta<?, V> propertyMeta) {
        DynamicComposite composite = keyFactory.createBaseForQuery(propertyMeta, EQUAL);
        return propertyMeta.getValueFromString(dao.getValue(key, composite));
    }

    protected <ID, V> List<V> loadListProperty(ID key, GenericDynamicCompositeDao<ID> dao,
            PropertyMeta<?, V> listPropertyMeta) {
        DynamicComposite start = keyFactory.createBaseForQuery(listPropertyMeta, EQUAL);
        DynamicComposite end = keyFactory.createBaseForQuery(listPropertyMeta, GREATER_THAN_EQUAL);
        List<Pair<DynamicComposite, String>> columns = dao
                .findColumnsRange(key, start, end, false, Integer.MAX_VALUE);
        List<V> list = null;
        if (columns.size() > 0) {
            list = listPropertyMeta.newListInstance();
            for (Pair<DynamicComposite, String> pair : columns) {
                list.add(listPropertyMeta.getValueFromString(pair.right));
            }
        }
        return list;
    }

    protected <ID, V> Set<V> loadSetProperty(ID key, GenericDynamicCompositeDao<ID> dao,
            PropertyMeta<?, V> setPropertyMeta) {

        DynamicComposite start = keyFactory.createBaseForQuery(setPropertyMeta, EQUAL);
        DynamicComposite end = keyFactory.createBaseForQuery(setPropertyMeta, GREATER_THAN_EQUAL);
        List<Pair<DynamicComposite, String>> columns = dao
                .findColumnsRange(key, start, end, false, Integer.MAX_VALUE);
        Set<V> set = null;
        if (columns.size() > 0) {
            set = setPropertyMeta.newSetInstance();
            for (Pair<DynamicComposite, String> pair : columns) {
                set.add(setPropertyMeta.getValueFromString(pair.right));
            }
        }
        return set;
    }

    protected <ID, K, V> Map<K, V> loadMapProperty(ID key, GenericDynamicCompositeDao<ID> dao,
            PropertyMeta<K, V> mapPropertyMeta) {

        DynamicComposite start = keyFactory.createBaseForQuery(mapPropertyMeta, EQUAL);
        DynamicComposite end = keyFactory.createBaseForQuery(mapPropertyMeta, GREATER_THAN_EQUAL);
        List<Pair<DynamicComposite, String>> columns = dao
                .findColumnsRange(key, start, end, false, Integer.MAX_VALUE);

        Class<K> keyClass = mapPropertyMeta.getKeyClass();
        Map<K, V> map = null;
        if (columns.size() > 0) {
            map = mapPropertyMeta.newMapInstance();
            for (Pair<DynamicComposite, String> pair : columns) {
                KeyValue<K, V> holder = mapPropertyMeta.getKeyValueFromString(pair.right);

                map.put(keyClass.cast(holder.getKey()), mapPropertyMeta.getValueFromString(holder.getValue()));
            }
        }
        return map;
    }

    public <ID, V> void loadPropertyIntoObject(Object realObject, ID key, GenericDynamicCompositeDao<ID> dao,
            PropertyMeta<?, V> propertyMeta) {
        Object value = null;
        switch (propertyMeta.type()) {
            case SIMPLE:
            case LAZY_SIMPLE:
                value = this.loadSimpleProperty(key, dao, propertyMeta);
                break;
            case LIST:
            case LAZY_LIST:
                value = this.loadListProperty(key, dao, propertyMeta);
                break;
            case SET:
            case LAZY_SET:
                value = this.loadSetProperty(key, dao, propertyMeta);
                break;
            case MAP:
            case LAZY_MAP:
                value = this.loadMapProperty(key, dao, propertyMeta);
                break;
            case JOIN_SIMPLE:
                value = this.loadJoinSimple(key, dao, propertyMeta);
                break;
            case JOIN_LIST:
                value = joinLoader.loadJoinListProperty(key, dao, propertyMeta);
                break;
            case JOIN_SET:
                value = joinLoader.loadJoinSetProperty(key, dao, propertyMeta);
                break;
            case JOIN_MAP:
                value = joinLoader.loadJoinMapProperty(key, dao, propertyMeta);
                break;
            default:
                return;
        }
        helper.setValueToField(realObject, propertyMeta.getSetter(), value);
    }

    @SuppressWarnings("unchecked")
    public <ID, JOIN_ID, V> V loadJoinSimple(ID key, GenericDynamicCompositeDao<ID> dao,
            PropertyMeta<?, V> propertyMeta) {
        EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();
        PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta.joinIdMeta();

        DynamicComposite composite = keyFactory.createBaseForQuery(propertyMeta, EQUAL);

        String stringJoinId = dao.getValue(key, composite);

        if (stringJoinId != null) {
            JOIN_ID joinId = joinIdMeta.getValueFromString(stringJoinId);
            return this.load(propertyMeta.getValueClass(), joinId, joinMeta);

        } else {
            return null;
        }

    }
}
