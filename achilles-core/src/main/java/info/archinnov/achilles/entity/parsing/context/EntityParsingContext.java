package info.archinnov.achilles.entity.parsing.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ConfigurationContext.Impl;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * EntityParsingContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParsingContext
{
    private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled;
    private ConfigurationContext configContext;
    private Boolean hasCounter = false;

    private Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
    private List<PropertyMeta<?, ?>> counterMetas = new ArrayList<PropertyMeta<?, ?>>();
    private Map<PropertyMeta<?, ?>, String> wideMaps = new HashMap<PropertyMeta<?, ?>, String>();
    private Map<PropertyMeta<?, ?>, String> joinWideMaps = new HashMap<PropertyMeta<?, ?>, String>();
    private Class<?> currentEntityClass;
    private ObjectMapper currentObjectMapper;
    private Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels;
    private boolean clusteredEntity = false;
    private String currentColumnFamilyName;

    public EntityParsingContext(//
            Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled, //
            ConfigurationContext configContext, //
            Class<?> currentEntityClass)
    {
        this.joinPropertyMetaToBeFilled = joinPropertyMetaToBeFilled;
        this.configContext = configContext;
        this.currentEntityClass = currentEntityClass;
    }

    public EntityParsingContext( //
            Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled, //
            ConfigurationContext configContext)
    {
        this.joinPropertyMetaToBeFilled = joinPropertyMetaToBeFilled;
        this.configContext = configContext;
    }

    public PropertyParsingContext newPropertyContext(Field currentField)
    {
        return new PropertyParsingContext(this, currentField);
    }

    public boolean isThriftImpl()
    {
        return configContext.getImpl() == Impl.THRIFT;
    }

    public Map<PropertyMeta<?, ?>, Class<?>> getJoinPropertyMetaToBeFilled()
    {
        return joinPropertyMetaToBeFilled;
    }

    public Class<?> getCurrentEntityClass()
    {
        return currentEntityClass;
    }

    public Map<String, PropertyMeta<?, ?>> getPropertyMetas()
    {
        return propertyMetas;
    }

    public Map<PropertyMeta<?, ?>, String> getWideMaps()
    {
        return wideMaps;
    }

    public Map<PropertyMeta<?, ?>, String> getJoinWideMaps()
    {
        return joinWideMaps;
    }

    public void setPropertyMetas(Map<String, PropertyMeta<?, ?>> propertyMetas)
    {
        this.propertyMetas = propertyMetas;
    }

    public Boolean getHasSimpleCounter()
    {
        return hasCounter;
    }

    public void setHasSimpleCounter(Boolean hasCounter)
    {
        this.hasCounter = hasCounter;
    }

    public ObjectMapper getCurrentObjectMapper()
    {
        return currentObjectMapper;
    }

    public void setCurrentObjectMapper(ObjectMapper currentObjectMapper)
    {
        this.currentObjectMapper = currentObjectMapper;
    }

    public boolean isClusteredEntity()
    {
        return clusteredEntity;
    }

    public void setClusteredEntity(boolean wideRow)
    {
        this.clusteredEntity = wideRow;
    }

    public void setCurrentConsistencyLevels(
            Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels)
    {
        this.currentConsistencyLevels = currentConsistencyLevels;
    }

    public List<PropertyMeta<?, ?>> getCounterMetas()
    {
        return counterMetas;
    }

    public Pair<ConsistencyLevel, ConsistencyLevel> getCurrentConsistencyLevels()
    {
        return currentConsistencyLevels;
    }

    public String getCurrentColumnFamilyName()
    {
        return currentColumnFamilyName;
    }

    public void setCurrentColumnFamilyName(String currentColumnFamilyName)
    {
        this.currentColumnFamilyName = currentColumnFamilyName;
    }

    public ObjectMapperFactory getObjectMapperFactory()
    {
        return configContext.getObjectMapperFactory();
    }

    public AchillesConsistencyLevelPolicy getConfigurableCLPolicy()
    {
        return configContext.getConsistencyPolicy();
    }
}
