package info.archinnov.achilles.entity.parser.context;

import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * PropertyParsingContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyParsingContext
{
	private EntityParsingContext context;
	private Field currentField;
	private String currentPropertyName;
	private String currentExternalTableName;
	private boolean joinColumn = false;
	private boolean isCustomConsistencyLevels;
	private boolean counterType;
	private boolean primaryKey = false;

	public PropertyParsingContext(EntityParsingContext context, //
			Field currentField)
	{
		this.context = context;
		this.currentField = currentField;
	}

	public ObjectMapper getCurrentObjectMapper()
	{
		return context.getCurrentObjectMapper();
	}

	public Map<PropertyMeta<?, ?>, String> getWideMaps()
	{
		return context.getWideMaps();
	}

	public Map<PropertyMeta<?, ?>, String> getJoinWideMaps()
	{
		return context.getJoinWideMaps();
	}

	public Map<String, PropertyMeta<?, ?>> getPropertyMetas()
	{
		return context.getPropertyMetas();
	}

	public Class<?> getCurrentEntityClass()
	{
		return context.getCurrentEntityClass();
	}

	public boolean isColumnFamilyDirectMapping()
	{
		return context.isColumnFamilyDirectMapping();
	}

	public Field getCurrentField()
	{
		return currentField;
	}

	public boolean isJoinColumn()
	{
		return joinColumn;
	}

	public void setJoinColumn(boolean joinColumn)
	{
		this.joinColumn = joinColumn;
	}

	public String getCurrentPropertyName()
	{
		return currentPropertyName;
	}

	public void setCurrentPropertyName(String currentPropertyName)
	{
		this.currentPropertyName = currentPropertyName;
	}

	public String getCurrentExternalTableName()
	{
		return currentExternalTableName;
	}

	public void setCurrentExternalTableName(String currentExternalTableName)
	{
		this.currentExternalTableName = currentExternalTableName;
	}

	public List<PropertyMeta<?, ?>> getCounterMetas()
	{
		return context.getCounterMetas();
	}

	public Pair<ConsistencyLevel, ConsistencyLevel> getCurrentConsistencyLevels()
	{
		return context.getCurrentConsistencyLevels();
	}

	public AchillesConfigurableConsistencyLevelPolicy getConfigurableCLPolicy()
	{
		return context.getConfigurableCLPolicy();
	}

	public Map<PropertyMeta<?, ?>, Class<?>> getJoinPropertyMetaToBeFilled()
	{
		return context.getJoinPropertyMetaToBeFilled();
	}

	public String getCurrentColumnFamilyName()
	{
		return context.getCurrentColumnFamilyName();
	}

	public boolean isExternal()
	{
		return !StringUtils.isBlank(currentExternalTableName);
	}

	public boolean isCustomConsistencyLevels()
	{
		return isCustomConsistencyLevels;
	}

	public void setCustomConsistencyLevels(boolean isCustomConsistencyLevels)
	{
		this.isCustomConsistencyLevels = isCustomConsistencyLevels;
	}

	public boolean isCounterType()
	{
		return counterType;
	}

	public void setCounterType(boolean counterType)
	{
		this.counterType = counterType;
		context.setHasCounter(counterType);
	}

	public boolean isPrimaryKey()
	{
		return primaryKey;
	}

	public void setPrimaryKey(boolean primaryKey)
	{
		this.primaryKey = primaryKey;
	}
}
