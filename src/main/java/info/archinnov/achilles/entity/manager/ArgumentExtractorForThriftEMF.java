package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
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
import me.prettyprint.hector.api.HConsistencyLevel;
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

	public Keyspace initKeyspace(Cluster cluster,
			AchillesConfigurableConsistencyLevelPolicy consistencyPolicy,
			Map<String, Object> configurationMap)
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
		keyspace.setConsistencyLevelPolicy(consistencyPolicy);
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

	public boolean ensureConsistencyOnJoin(Map<String, Object> configurationMap)
	{
		Boolean ensureConsistencyOnJoin = (Boolean) configurationMap
				.get(ENSURE_CONSISTENCY_ON_JOIN_PARAM);
		if (ensureConsistencyOnJoin != null)
		{
			return ensureConsistencyOnJoin;
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
	public Map<String, HConsistencyLevel> initReadConsistencyMap(Map<String, Object> configMap)
	{
		Map<String, String> readConsistencyMap = (Map<String, String>) configMap
				.get(READ_CONSISTENCY_MAP_PARAM);

		return parseConsistencyLevelMap(readConsistencyMap);
	}

	@SuppressWarnings("unchecked")
	public Map<String, HConsistencyLevel> initWriteConsistencyMap(Map<String, Object> configMap)
	{
		Map<String, String> writeConsistencyMap = (Map<String, String>) configMap
				.get(WRITE_CONSISTENCY_MAP_PARAM);

		return parseConsistencyLevelMap(writeConsistencyMap);
	}

	private Map<String, HConsistencyLevel> parseConsistencyLevelMap(
			Map<String, String> consistencyLevelMap)
	{
		Map<String, HConsistencyLevel> map = new HashMap<String, HConsistencyLevel>();
		if (consistencyLevelMap != null && !consistencyLevelMap.isEmpty())
		{
			for (Entry<String, String> entry : consistencyLevelMap.entrySet())
			{
				map.put(entry.getKey(), parseConsistencyLevelOrGetDefault(entry.getValue())
						.getHectorLevel());
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
