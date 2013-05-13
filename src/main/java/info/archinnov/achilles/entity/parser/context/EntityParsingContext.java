package info.archinnov.achilles.entity.parser.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.context.AchillesConfigurationContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * ParsingContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParsingContext
{
	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled;
	private AchillesConfigurationContext configContext;
	private Boolean hasCounter = false;

	private Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
	private List<PropertyMeta<?, ?>> counterMetas = new ArrayList<PropertyMeta<?, ?>>();
	private Map<PropertyMeta<?, ?>, String> wideMaps = new HashMap<PropertyMeta<?, ?>, String>();
	private Map<PropertyMeta<?, ?>, String> joinWideMaps = new HashMap<PropertyMeta<?, ?>, String>();
	private Class<?> currentEntityClass;
	private ObjectMapper currentObjectMapper;
	private Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels;
	private boolean wideRow = false;
	private String currentColumnFamilyName;

	public EntityParsingContext(//
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled, //
			AchillesConfigurationContext configContext, //
			Class<?> currentEntityClass)
	{
		this.joinPropertyMetaToBeFilled = joinPropertyMetaToBeFilled;
		this.configContext = configContext;
		this.currentEntityClass = currentEntityClass;
	}

	public EntityParsingContext( //
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled, //
			AchillesConfigurationContext configContext)
	{
		this.joinPropertyMetaToBeFilled = joinPropertyMetaToBeFilled;
		this.configContext = configContext;
	}

	public PropertyParsingContext newPropertyContext(Field currentField)
	{
		return new PropertyParsingContext(this, currentField);
	}

	public Map<PropertyMeta<?, ?>, Class<?>> getJoinPropertyMetaToBeFilled()
	{
		return joinPropertyMetaToBeFilled;
	}

	public AchillesConfigurationContext getConfigContext()
	{
		return configContext;
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

	public boolean isWideRow()
	{
		return wideRow;
	}

	public void setWideRow(boolean wideRow)
	{
		this.wideRow = wideRow;
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
