package info.archinnov.achilles.integration.spring;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static org.apache.commons.lang.StringUtils.isBlank;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * AbstractThriftEntityManagerFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class AbstractThriftEntityManagerFactory
{
	protected String entityPackages;

	protected Cluster cluster;
	protected Keyspace keyspace;

	protected String cassandraHost;
	protected String clusterName;
	protected String keyspaceName;

	protected ObjectMapperFactory objectMapperFactory;
	protected ObjectMapper objectMapper;

	protected ConsistencyLevel defaultReadLevel;
	protected ConsistencyLevel defaultWriteLevel;
	protected Map<String, ConsistencyLevel> readLevelMap;
	protected Map<String, ConsistencyLevel> writeLevelMap;

	protected boolean forceColumnFamilyCreation = false;
	protected boolean ensureJoinConsistency = false;

	protected ThriftEntityManager em;

	@PostConstruct
	public void initialize()
	{
		Map<String, Object> configMap = new HashMap<String, Object>();

		fillEntityPackages(configMap);

		fillClusterAndKeyspace(configMap);

		fillObjectMapper(configMap);

		fillConsistencyLevels(configMap);

		configMap.put(FORCE_CF_CREATION_PARAM, forceColumnFamilyCreation);
		configMap.put(ENSURE_CONSISTENCY_ON_JOIN_PARAM, ensureJoinConsistency);

		ThriftEntityManagerFactory factory = new ThriftEntityManagerFactory(configMap);
		em = (ThriftEntityManager) factory.createEntityManager();
	}

	private void fillEntityPackages(Map<String, Object> configMap)
	{
		if (isBlank(entityPackages))
		{
			throw new IllegalArgumentException(
					"Entity packages should be provided for entity scanning");
		}
		configMap.put(ENTITY_PACKAGES_PARAM, entityPackages);
	}

	private void fillClusterAndKeyspace(Map<String, Object> configMap)
	{
		if (cluster != null)
		{
			configMap.put(CLUSTER_PARAM, cluster);
		}
		else
		{
			if (isBlank(cassandraHost) || isBlank(clusterName))
			{
				throw new IllegalArgumentException(
						"Either a Cassandra cluster or hostname:port & clusterName should be provided");
			}
			configMap.put(HOSTNAME_PARAM, cassandraHost);
			configMap.put(CLUSTER_NAME_PARAM, clusterName);
		}

		if (keyspace != null)
		{
			configMap.put(KEYSPACE_PARAM, keyspace);
		}
		else
		{
			if (isBlank(keyspaceName))
			{
				throw new IllegalArgumentException(
						"Either a Cassandra keyspace or keyspaceName should be provided");
			}
			configMap.put(KEYSPACE_NAME_PARAM, keyspaceName);
		}
	}

	private void fillObjectMapper(Map<String, Object> configMap)
	{
		if (objectMapperFactory != null)
		{
			configMap.put(OBJECT_MAPPER_FACTORY_PARAM, objectMapperFactory);
		}
		if (objectMapper != null)
		{
			configMap.put(OBJECT_MAPPER_PARAM, objectMapper);
		}
	}

	private void fillConsistencyLevels(Map<String, Object> configMap)
	{
		if (defaultReadLevel != null)
		{
			configMap.put(DEFAUT_READ_CONSISTENCY_PARAM, defaultReadLevel);
		}
		if (defaultWriteLevel != null)
		{
			configMap.put(DEFAUT_WRITE_CONSISTENCY_PARAM, defaultWriteLevel);
		}

		if (readLevelMap != null)
		{
			configMap.put(READ_CONSISTENCY_MAP_PARAM, readLevelMap);
		}
		if (writeLevelMap != null)
		{
			configMap.put(WRITE_CONSISTENCY_MAP_PARAM, writeLevelMap);
		}
	}

	public void setCluster(Cluster cluster)
	{
		this.cluster = cluster;
	}

	public void setKeyspace(Keyspace keyspace)
	{
		this.keyspace = keyspace;
	}

	public void setCassandraHost(String cassandraHost)
	{
		this.cassandraHost = cassandraHost;
	}

	public void setClusterName(String clusterName)
	{
		this.clusterName = clusterName;
	}

	public void setKeyspaceName(String keyspaceName)
	{
		this.keyspaceName = keyspaceName;
	}

	public void setEntityPackages(String entityPackages)
	{
		this.entityPackages = entityPackages;
	}

	public void setForceColumnFamilyCreation(boolean forceColumnFamilyCreation)
	{
		this.forceColumnFamilyCreation = forceColumnFamilyCreation;
	}

	public void setObjectMapperFactory(ObjectMapperFactory objectMapperFactory)
	{
		this.objectMapperFactory = objectMapperFactory;
	}

	public void setObjectMapper(ObjectMapper objectMapper)
	{
		this.objectMapper = objectMapper;
	}

	public void setDefaultReadLevel(ConsistencyLevel defaultReadLevel)
	{
		this.defaultReadLevel = defaultReadLevel;
	}

	public void setDefaultWriteLevel(ConsistencyLevel defaultWriteLevel)
	{
		this.defaultWriteLevel = defaultWriteLevel;
	}

	public void setReadLevelMap(Map<String, ConsistencyLevel> readLevelMap)
	{
		this.readLevelMap = readLevelMap;
	}

	public void setWriteLevelMap(Map<String, ConsistencyLevel> writeLevelMap)
	{
		this.writeLevelMap = writeLevelMap;
	}

	public void setEnsureJoinConsistency(boolean ensureJoinConsistency)
	{
		this.ensureJoinConsistency = ensureJoinConsistency;
	}
}
