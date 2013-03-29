package info.archinnov.achilles.entity.parser;

import static org.mockito.Mockito.mock;
import info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * EntityParsingContextTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParsingContextTestBuilder
{
	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled;
	private Map<String, GenericDynamicCompositeDao<?>> entityDaosMap;
	private Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap;
	private Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
	private Map<PropertyMeta<?, ?>, String> externalWideMaps = new HashMap<PropertyMeta<?, ?>, String>();
	private Map<PropertyMeta<?, ?>, String> joinExternalWideMaps = new HashMap<PropertyMeta<?, ?>, String>();

	private Cluster cluster;
	private Keyspace keyspace;
	private AchillesConfigurableConsistencyLevelPolicy configurableCLPolicy;
	private ObjectMapperFactory objectMapperFactory;
	private CounterDao counterDao;
	private Boolean hasCounter = false;

	private Class<?> currentEntityClass;
	private ObjectMapper currentObjectMapper;
	private Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels;
	private boolean columnFamilyDirectMapping = false;
	private String currentColumnFamilyName;

	public static EntityParsingContextTestBuilder context( //
			Cluster cluster, Keyspace keyspace, //
			AchillesConfigurableConsistencyLevelPolicy configurableCLPolicy, //
			ObjectMapperFactory objectMapperFactory, //
			CounterDao counterDao, Class<?> currentEntityClass)
	{
		return new EntityParsingContextTestBuilder(cluster, keyspace, configurableCLPolicy,
				objectMapperFactory, counterDao, currentEntityClass);
	}

	public static EntityParsingContextTestBuilder mockAll(Class<?> currentEntityClass)
	{
		return new EntityParsingContextTestBuilder(mock(Cluster.class),//
				mock(Keyspace.class), mock(AchillesConfigurableConsistencyLevelPolicy.class), //
				mock(ObjectMapperFactory.class), mock(CounterDao.class), currentEntityClass);
	}

	public EntityParsingContextTestBuilder(Cluster cluster, Keyspace keyspace,
			AchillesConfigurableConsistencyLevelPolicy configurableCLPolicy,
			ObjectMapperFactory objectMapperFactory, CounterDao counterDao,
			Class<?> currentEntityClass)
	{
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.configurableCLPolicy = configurableCLPolicy;
		this.objectMapperFactory = objectMapperFactory;
		this.counterDao = counterDao;
		this.currentEntityClass = currentEntityClass;
	}

	public EntityParsingContextTestBuilder joinPropertyMetaToBeFilled(
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled)
	{
		this.joinPropertyMetaToBeFilled = joinPropertyMetaToBeFilled;
		return this;
	}

	public EntityParsingContextTestBuilder entityDaosMap(
			Map<String, GenericDynamicCompositeDao<?>> entityDaosMap)
	{
		this.entityDaosMap = entityDaosMap;
		return this;
	}

	public EntityParsingContextTestBuilder columnFamilyDaosMap(
			Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap)
	{
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		return this;
	}

	public EntityParsingContextTestBuilder propertyMetas(
			Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		this.propertyMetas = propertyMetas;
		return this;
	}

	public EntityParsingContextTestBuilder externalWideMaps(
			Map<PropertyMeta<?, ?>, String> externalWideMaps)
	{
		this.externalWideMaps = externalWideMaps;
		return this;
	}

	public EntityParsingContextTestBuilder joinExternalWideMaps(
			Map<PropertyMeta<?, ?>, String> joinExternalWideMaps)
	{
		this.joinExternalWideMaps = joinExternalWideMaps;
		return this;
	}

	public EntityParsingContextTestBuilder currentObjectMapper(ObjectMapper currentObjectMapper)
	{
		this.currentObjectMapper = currentObjectMapper;
		return this;
	}

	public EntityParsingContextTestBuilder currentConsistencyLevels(
			Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels)
	{
		this.currentConsistencyLevels = currentConsistencyLevels;
		return this;
	}

	public EntityParsingContextTestBuilder columnFamilyDirectMapping(
			boolean columnFamilyDirectMapping)
	{
		this.columnFamilyDirectMapping = columnFamilyDirectMapping;
		return this;
	}

	public EntityParsingContextTestBuilder hasCounter(boolean hasCounter)
	{
		this.hasCounter = hasCounter;
		return this;
	}

	public EntityParsingContextTestBuilder currentColumnFamilyName(String currentColumnFamilyName)
	{
		this.currentColumnFamilyName = currentColumnFamilyName;
		return this;
	}

	public EntityParsingContext build()
	{
		EntityParsingContext context = new EntityParsingContext(joinPropertyMetaToBeFilled, //
				entityDaosMap, columnFamilyDaosMap, configurableCLPolicy, counterDao, cluster,//
				keyspace, objectMapperFactory, currentEntityClass);

		context.setPropertyMetas(propertyMetas);
		context.setExternalWideMaps(externalWideMaps);
		context.setJoinExternalWideMaps(joinExternalWideMaps);
		context.setCurrentObjectMapper(currentObjectMapper);
		context.setCurrentConsistencyLevels(currentConsistencyLevels);
		context.setColumnFamilyDirectMapping(columnFamilyDirectMapping);
		context.setCurrentColumnFamilyName(currentColumnFamilyName);
		context.setHasCounter(hasCounter);
		return context;
	}
}
