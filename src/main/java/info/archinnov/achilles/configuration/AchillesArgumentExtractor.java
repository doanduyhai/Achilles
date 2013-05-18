package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.AchillesConfigurationParameters.*;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.json.DefaultObjectMapperFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * AchillesArgumentExtractor
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AchillesArgumentExtractor
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

	public ConsistencyLevel initDefaultReadConsistencyLevel(Map<String, Object> configMap)
	{
		String defaultReadLevel = (String) configMap.get(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultReadLevel);
	}

	public ConsistencyLevel initDefaultWriteConsistencyLevel(Map<String, Object> configMap)
	{
		String defaultWriteLevel = (String) configMap.get(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultWriteLevel);
	}

	public Map<String, ConsistencyLevel> initReadConsistencyMap(Map<String, Object> configMap)
	{
		Map<String, String> readConsistencyMap = (Map<String, String>) configMap
				.get(CONSISTENCY_LEVEL_READ_MAP_PARAM);

		return parseConsistencyLevelMap(readConsistencyMap);
	}

	public Map<String, ConsistencyLevel> initWriteConsistencyMap(Map<String, Object> configMap)
	{
		Map<String, String> writeConsistencyMap = (Map<String, String>) configMap
				.get(CONSISTENCY_LEVEL_WRITE_MAP_PARAM);

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
}
