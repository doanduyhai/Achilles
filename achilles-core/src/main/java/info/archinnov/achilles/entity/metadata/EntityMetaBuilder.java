package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.metadata.PropertyType.eagerType;
import static info.archinnov.achilles.table.TableCreator.TABLE_PATTERN;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.FluentIterable;

/**
 * EntityMetaBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMetaBuilder
{
    private static final Logger log = LoggerFactory.getLogger(EntityMetaBuilder.class);

    private PropertyMeta<?, ?> idMeta;
    private Class<?> entityClass;
    private String className;
    private String columnFamilyName;
    private Map<String, PropertyMeta<?, ?>> propertyMetas;
    private boolean clusteredEntity = false;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

    public static EntityMetaBuilder entityMetaBuilder(PropertyMeta<?, ?> idMeta)
    {
        return new EntityMetaBuilder(idMeta);
    }

    public EntityMetaBuilder(PropertyMeta<?, ?> idMeta) {
        this.idMeta = idMeta;
    }

    public EntityMeta build()
    {
        log.debug("Build entityMeta for entity class {}", className);

        Validator.validateNotNull(idMeta, "idMeta should not be null");
        Validator.validateNotEmpty(propertyMetas, "propertyMetas map should not be empty");
        Validator.validateRegExp(columnFamilyName, TABLE_PATTERN, "columnFamilyName");

        EntityMeta meta = new EntityMeta();

        meta.setIdMeta(idMeta);
        meta.setIdClass(idMeta.getValueClass());
        meta.setEntityClass(entityClass);
        meta.setClassName(className);
        meta.setTableName(columnFamilyName);
        meta.setPropertyMetas(Collections.unmodifiableMap(propertyMetas));
        meta.setGetterMetas(Collections.unmodifiableMap(extractGetterMetas(propertyMetas)));
        meta.setSetterMetas(Collections.unmodifiableMap(extractSetterMetas(propertyMetas)));
        meta.setClusteredEntity(clusteredEntity);
        meta.setConsistencyLevels(consistencyLevels);

        List<PropertyMeta<?, ?>> eagerMetas = FluentIterable
                .from(propertyMetas.values())
                .filter(eagerType)
                .toImmutableList();

        meta.setEagerMetas(eagerMetas);
        meta.setEagerGetters(Collections.unmodifiableList(extractEagerGetters(eagerMetas)));

        return meta;
    }

    private Map<Method, PropertyMeta<?, ?>> extractGetterMetas(
            Map<String, PropertyMeta<?, ?>> propertyMetas)
    {
        Map<Method, PropertyMeta<?, ?>> getterMetas = new HashMap<Method, PropertyMeta<?, ?>>();
        for (PropertyMeta<?, ?> propertyMeta : propertyMetas.values())
        {
            getterMetas.put(propertyMeta.getGetter(), propertyMeta);
        }
        return getterMetas;
    }

    private Map<Method, PropertyMeta<?, ?>> extractSetterMetas(
            Map<String, PropertyMeta<?, ?>> propertyMetas)
    {
        Map<Method, PropertyMeta<?, ?>> setterMetas = new HashMap<Method, PropertyMeta<?, ?>>();
        for (PropertyMeta<?, ?> propertyMeta : propertyMetas.values())
        {
            setterMetas.put(propertyMeta.getSetter(), propertyMeta);
        }
        return setterMetas;
    }

    private List<Method> extractEagerGetters(List<PropertyMeta<?, ?>> eagerMetas)
    {
        List<Method> eagerMethods = new ArrayList<Method>();
        for (PropertyMeta<?, ?> propertyMeta : eagerMetas)
        {
            eagerMethods.add(propertyMeta.getGetter());
        }
        return eagerMethods;

    }

    public EntityMetaBuilder entityClass(Class<?> entityClass)
    {
        this.entityClass = entityClass;
        return this;
    }

    public EntityMetaBuilder className(String className)
    {
        this.className = className;
        return this;
    }

    public EntityMetaBuilder columnFamilyName(String columnFamilyName)
    {
        this.columnFamilyName = columnFamilyName;
        return this;
    }

    public EntityMetaBuilder propertyMetas(Map<String, PropertyMeta<?, ?>> propertyMetas)
    {
        this.propertyMetas = propertyMetas;
        return this;
    }

    public EntityMetaBuilder clusteredEntity(boolean clusteredEntity)
    {
        this.clusteredEntity = clusteredEntity;
        return this;
    }

    public EntityMetaBuilder consistencyLevels(
            Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
    {
        this.consistencyLevels = consistencyLevels;
        return this;
    }
}
