package info.archinnov.achilles.entity.metadata;

import info.archinnov.achilles.type.ConsistencyLevel;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import com.google.common.base.Predicate;

/**
 * EntityMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMeta {

    public static final Predicate<EntityMeta> clusteredCounterFilter = new Predicate<EntityMeta>() {
        @Override
        public boolean apply(EntityMeta meta)
        {
            return meta.isClusteredCounter();
        }
    };

    public static final Predicate<EntityMeta> excludeClusteredCounterFilter = new Predicate<EntityMeta>() {
        @Override
        public boolean apply(EntityMeta meta)
        {
            return !meta.isClusteredCounter();
        }
    };

    private Class<?> entityClass;
    private String className;
    private String tableName;
    private Class<?> idClass;
    private Map<String, PropertyMeta<?, ?>> propertyMetas;
    private List<PropertyMeta<?, ?>> eagerMetas;
    private List<Method> eagerGetters;
    private PropertyMeta<?, ?> idMeta;
    private Map<Method, PropertyMeta<?, ?>> getterMetas;
    private Map<Method, PropertyMeta<?, ?>> setterMetas;
    private boolean clusteredEntity = false;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
    private PropertyMeta<?, ?> firstMeta;
    private List<PropertyMeta<?, ?>> allMetasExceptIdMeta;
    private boolean clusteredCounter = false;

    public Class<?> getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class<?> entityClass) {
        this.entityClass = entityClass;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, PropertyMeta<?, ?>> getPropertyMetas() {
        return propertyMetas;
    }

    public void setPropertyMetas(Map<String, PropertyMeta<?, ?>> propertyMetas) {
        this.propertyMetas = propertyMetas;
    }

    public PropertyMeta<?, ?> getIdMeta() {
        return idMeta;
    }

    public void setIdMeta(PropertyMeta<?, ?> idMeta) {
        this.idMeta = idMeta;
    }

    public Map<Method, PropertyMeta<?, ?>> getGetterMetas() {
        return getterMetas;
    }

    public void setGetterMetas(Map<Method, PropertyMeta<?, ?>> getterMetas) {
        this.getterMetas = getterMetas;
    }

    public Map<Method, PropertyMeta<?, ?>> getSetterMetas() {
        return setterMetas;
    }

    public void setSetterMetas(Map<Method, PropertyMeta<?, ?>> setterMetas) {
        this.setterMetas = setterMetas;
    }

    public boolean isClusteredEntity() {
        return clusteredEntity;
    }

    public void setClusteredEntity(boolean clusteredEntity) {
        this.clusteredEntity = clusteredEntity;
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return this.consistencyLevels.left;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return this.consistencyLevels.right;
    }

    public Pair<ConsistencyLevel, ConsistencyLevel> getConsistencyLevels() {
        return this.consistencyLevels;
    }

    public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
    }

    public Class<?> getIdClass() {
        return idClass;
    }

    public void setIdClass(Class<?> idClass) {
        this.idClass = idClass;
    }

    public List<PropertyMeta<?, ?>> getEagerMetas() {
        return eagerMetas;
    }

    public void setEagerMetas(List<PropertyMeta<?, ?>> eagerMetas) {
        this.eagerMetas = eagerMetas;
    }

    public List<Method> getEagerGetters() {
        return eagerGetters;
    }

    public void setEagerGetters(List<Method> eagerGetters) {
        this.eagerGetters = eagerGetters;
    }

    public PropertyMeta<?, ?> getFirstMeta() {

        return this.firstMeta;
    }

    public void setFirstMeta(PropertyMeta<?, ?> firstMeta) {
        this.firstMeta = firstMeta;
    }

    public List<PropertyMeta<?, ?>> getAllMetas() {
        return new ArrayList<PropertyMeta<?, ?>>(propertyMetas.values());
    }

    public List<PropertyMeta<?, ?>> getAllMetasExceptIdMeta() {

        return this.allMetasExceptIdMeta;
    }

    public void setAllMetasExceptIdMeta(List<PropertyMeta<?, ?>> allMetasExceptIdMeta) {
        this.allMetasExceptIdMeta = allMetasExceptIdMeta;
    }

    public boolean isClusteredCounter()
    {
        return this.clusteredCounter;
    }

    public void setClusteredCounter(boolean clusteredCounter) {
        this.clusteredCounter = clusteredCounter;
    }

    public boolean isValueless()
    {
        return propertyMetas.size() == 1;
    }

    @Override
    public String toString() {
        StringBuilder description = new StringBuilder();
        description.append("EntityMeta [className=").append(className).append(", ");
        description.append("columnFamilyName=").append(tableName).append(", ");
        description.append("propertyMetas=[").append(StringUtils.join(propertyMetas.keySet(), ",")).append("], ");
        description.append("idMeta=").append(idMeta.toString()).append(", ");
        description.append("clusteredEntity=").append(clusteredEntity).append(", ");
        description.append("consistencyLevels=[").append(consistencyLevels.left.name()).append(",")
                .append(consistencyLevels.right.name()).append("]]");
        return description.toString();
    }
}
