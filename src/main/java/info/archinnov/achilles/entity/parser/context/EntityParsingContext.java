package info.archinnov.achilles.entity.parser.context;

import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

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
	private Map<String, GenericEntityDao<?>> entityDaosMap;
	private Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap;
	private AchillesConfigurableConsistencyLevelPolicy configurableCLPolicy;
	private Cluster cluster;
	private Keyspace keyspace;
	private ObjectMapperFactory objectMapperFactory;
	private Boolean hasCounter = false;

	private Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
	private List<PropertyMeta<?, ?>> counterMetas = new ArrayList<PropertyMeta<?, ?>>();
	private Map<PropertyMeta<?, ?>, String> wideMaps = new HashMap<PropertyMeta<?, ?>, String>();
	private Map<PropertyMeta<?, ?>, String> joinWideMaps = new HashMap<PropertyMeta<?, ?>, String>();
	private Class<?> currentEntityClass;
	private ObjectMapper currentObjectMapper;
	private Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels;
	private boolean columnFamilyDirectMapping = false;
	private String currentColumnFamilyName;

	public EntityParsingContext() {}

	public EntityParsingContext(//
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled, //
			Map<String, GenericEntityDao<?>> entityDaosMap, //
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, //
			AchillesConfigurableConsistencyLevelPolicy configurableCLPolicy, //
			Cluster cluster, Keyspace keyspace, //
			ObjectMapperFactory objectMapperFactory, Class<?> currentEntityClass)
	{
		this.joinPropertyMetaToBeFilled = joinPropertyMetaToBeFilled;
		this.entityDaosMap = entityDaosMap;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.configurableCLPolicy = configurableCLPolicy;
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.objectMapperFactory = objectMapperFactory;
		this.currentEntityClass = currentEntityClass;
	}

	public EntityParsingContext(//
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled, //
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap, //
			AchillesConfigurableConsistencyLevelPolicy configurableCLPolicy, //
			Cluster cluster, Keyspace keyspace)
	{
		this.joinPropertyMetaToBeFilled = joinPropertyMetaToBeFilled;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.configurableCLPolicy = configurableCLPolicy;
		this.cluster = cluster;
		this.keyspace = keyspace;
	}

	public PropertyParsingContext newPropertyContext(Field currentField)
	{
		return new PropertyParsingContext(this, currentField);
	}

	public Map<PropertyMeta<?, ?>, Class<?>> getJoinPropertyMetaToBeFilled()
	{
		return joinPropertyMetaToBeFilled;
	}

	public AchillesConfigurableConsistencyLevelPolicy getConfigurableCLPolicy()
	{
		return configurableCLPolicy;
	}

	public Cluster getCluster()
	{
		return cluster;
	}

	public Keyspace getKeyspace()
	{
		return keyspace;
	}

	public Class<?> getCurrentEntityClass()
	{
		return currentEntityClass;
	}

	public ObjectMapperFactory getObjectMapperFactory()
	{
		return objectMapperFactory;
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

	public boolean isColumnFamilyDirectMapping()
	{
		return columnFamilyDirectMapping;
	}

	public void setColumnFamilyDirectMapping(boolean columnFamilyDirectMapping)
	{
		this.columnFamilyDirectMapping = columnFamilyDirectMapping;
	}

	public void setCurrentConsistencyLevels(
			Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels)
	{
		this.currentConsistencyLevels = currentConsistencyLevels;
	}

	public Map<String, GenericEntityDao<?>> getEntityDaosMap()
	{
		return entityDaosMap;
	}

	public Map<String, GenericColumnFamilyDao<?, ?>> getColumnFamilyDaosMap()
	{
		return columnFamilyDaosMap;
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
}
