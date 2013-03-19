package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.json.DefaultObjectMapperFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.validation.Validator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * ThriftArgumentExtractor
 * 
 * @author DuyHai DOAN
 * 
 */
public class ArgumentExtractorForThriftEMF
{
	public static final String ENTITY_PACKAGES_PARAM = "achilles.entity.packages";

	public static final String HOSTNAME_PARAM = "achilles.cassandra.host";
	public static final String CLUSTER_NAME_PARAM = "achilles.cassandra.cluster.name";
	public static final String KEYSPACE_NAME_PARAM = "achilles.cassandra.keyspace.name";

	public static final String CLUSTER_PARAM = "achilles.cassandra.cluster";
	public static final String KEYSPACE_PARAM = "achilles.cassandra.keyspace";

	public static final String FORCE_CF_CREATION_PARAM = "achilles.ddl.force.column.family.creation";
	public static final String OBJECT_MAPPER_FACTORY_PARAM = "achilles.json.object.mapper.factory";
	public static final String OBJECT_MAPPER_PARAM = "achilles.json.object.mapper";

	public static final String DEFAUT_READ_CONSISTENCY_PARAM = "achilles.default.consistency.read";
	public static final String DEFAUT_WRITE_CONSISTENCY_PARAM = "achilles.default.consistency.write";
	public static final String READ_CONSISTENCY_MAP_PARAM = "achilles.consistency.read.map";
	public static final String WRITE_CONSISTENCY_MAP_PARAM = "achilles.consistency.write.map";

	private static final ConsistencyLevel DEFAULT_LEVEL = ConsistencyLevel.QUORUM;

	public List<String> initEntityPackages(Map<String, Object> configurationMap)
	{
		String entityPackages = (String) configurationMap.get(ENTITY_PACKAGES_PARAM);
		if (StringUtils.isBlank(entityPackages))
		{
			throw new AchillesException(
					"'"
							+ ENTITY_PACKAGES_PARAM
							+ "' property should be set for Achilles ThrifEntityManagerFactory bootstraping");
		}
		else
		{
			return Arrays.asList(StringUtils.split(entityPackages, ","));
		}
	}

	public Cluster initCluster(Map<String, Object> configurationMap)
	{
		Cluster cluster = (Cluster) configurationMap.get(CLUSTER_PARAM);
		if (cluster == null)
		{
			String cassandraHost = (String) configurationMap.get(HOSTNAME_PARAM);
			String cassandraClusterName = (String) configurationMap.get(CLUSTER_NAME_PARAM);

			Validator
					.validateNotBlank(
							cassandraHost,
							"Either '"
									+ CLUSTER_PARAM
									+ "' property or '"
									+ HOSTNAME_PARAM
									+ "'/'"
									+ CLUSTER_NAME_PARAM
									+ "' properties should be provided for Achilles ThrifEntityManagerFactory bootstraping");
			Validator
					.validateNotBlank(
							cassandraClusterName,
							"Either '"
									+ CLUSTER_PARAM
									+ "' property or '"
									+ HOSTNAME_PARAM
									+ "'/'"
									+ CLUSTER_NAME_PARAM
									+ "' properties should be provided for Achilles ThrifEntityManagerFactory bootstraping");

			cluster = HFactory.getOrCreateCluster(cassandraClusterName,
					new CassandraHostConfigurator(cassandraHost));
		}

		return cluster;
	}

	public Keyspace initKeyspace(Cluster cluster, Map<String, Object> configurationMap)
	{
		Keyspace keyspace = (Keyspace) configurationMap.get(KEYSPACE_PARAM);
		if (keyspace == null)
		{
			String keyspaceName = (String) configurationMap.get(KEYSPACE_NAME_PARAM);
			Validator
					.validateNotBlank(
							keyspaceName,
							"Either '"
									+ KEYSPACE_PARAM
									+ "' property or '"
									+ KEYSPACE_NAME_PARAM
									+ "' property should be provided for Achilles ThrifEntityManagerFactory bootstraping");

			keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		}

		return keyspace;
	}

	public boolean initForceCFCreation(Map<String, Object> configurationMap)
	{
		Boolean forceColumnFamilyCreation = (Boolean) configurationMap.get(FORCE_CF_CREATION_PARAM);
		if (forceColumnFamilyCreation != null)
		{
			return forceColumnFamilyCreation;
		}
		else
		{
			return false;
		}
	}

	public ObjectMapperFactory initObjectMapperFactory(Map<String, Object> configurationMap)
	{
		ObjectMapperFactory objectMapperFactory = (ObjectMapperFactory) configurationMap
				.get(OBJECT_MAPPER_FACTORY_PARAM);
		if (objectMapperFactory == null)
		{
			ObjectMapper mapper = (ObjectMapper) configurationMap.get(OBJECT_MAPPER_PARAM);
			if (mapper != null)
			{
				objectMapperFactory = factoryFromMapper(mapper);
			}
			else
			{
				objectMapperFactory = new DefaultObjectMapperFactory();
			}
		}

		return objectMapperFactory;
	}

	public ConsistencyLevel initDefaultReadConsistencyLevel(Map<String, Object> configMap)
	{
		String defaultReadLevel = (String) configMap.get(DEFAUT_READ_CONSISTENCY_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultReadLevel);
	}

	public ConsistencyLevel initDefaultWriteConsistencyLevel(Map<String, Object> configMap)
	{
		String defaultWriteLevel = (String) configMap.get(DEFAUT_WRITE_CONSISTENCY_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultWriteLevel);
	}

	@SuppressWarnings("unchecked")
	public Map<String, ConsistencyLevel> initReadConsistencyMap(Map<String, Object> configMap)
	{
		Map<String, String> readConsistencyMap = (Map<String, String>) configMap
				.get(READ_CONSISTENCY_MAP_PARAM);

		return parseConsistencyLevelMap(readConsistencyMap);
	}

	@SuppressWarnings("unchecked")
	public Map<String, ConsistencyLevel> initWriteConsistencyMap(Map<String, Object> configMap)
	{
		Map<String, String> writeConsistencyMap = (Map<String, String>) configMap
				.get(WRITE_CONSISTENCY_MAP_PARAM);

		return parseConsistencyLevelMap(writeConsistencyMap);
	}

	private Map<String, ConsistencyLevel> parseConsistencyLevelMap(
			Map<String, String> consistencyLevelMap)
	{
		Map<String, ConsistencyLevel> map = new HashMap<String, ConsistencyLevel>();
		if (consistencyLevelMap != null && !consistencyLevelMap.isEmpty())
		{
			for (Entry<String, String> entry : consistencyLevelMap.entrySet())
			{
				map.put(entry.getKey(), parseConsistencyLevelOrGetDefault(entry.getValue()));
			}
		}

		return map;
	}

	private ConsistencyLevel parseConsistencyLevelOrGetDefault(String consistencyLevel)
	{
		ConsistencyLevel level = DEFAULT_LEVEL;
		if (StringUtils.isNotBlank(consistencyLevel))
		{
			try
			{
				level = ConsistencyLevel.valueOf(consistencyLevel);
			}
			catch (IllegalArgumentException e)
			{
				throw new IllegalArgumentException("'" + consistencyLevel
						+ "' is not a valid Consistency Level");
			}
		}
		return level;
	}

	protected static ObjectMapperFactory factoryFromMapper(final ObjectMapper mapper)
	{
		return new ObjectMapperFactory()
		{
			@Override
			public <T> ObjectMapper getMapper(Class<T> type)
			{
				return mapper;
			}
		};
	}

}
