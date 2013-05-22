package info.archinnov.achilles.integration.spring;

import static info.archinnov.achilles.configuration.AchillesConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.*;
import static org.apache.commons.lang.StringUtils.isBlank;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.FactoryBean;

/**
 * ThriftEntityManagerFactoryBean
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityManagerFactoryBean implements FactoryBean<ThriftEntityManager>
{
	private String entityPackages;

	private Cluster cluster;
	private Keyspace keyspace;

	private String cassandraHost;
	private String clusterName;
	private String keyspaceName;

	private ObjectMapperFactory objectMapperFactory;
	private ObjectMapper objectMapper;

	private String consistencyLevelReadDefault;
	private String consistencyLevelWriteDefault;
	private Map<String, String> consistencyLevelReadMap;
	private Map<String, String> consistencyLevelWriteMap;

	private boolean forceColumnFamilyCreation = false;
	private boolean ensureJoinConsistency = false;

	private ThriftEntityManager em;

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
		if (consistencyLevelReadDefault != null)
		{
			configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM, consistencyLevelReadDefault);
		}
		if (consistencyLevelWriteDefault != null)
		{
			configMap.put(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM, consistencyLevelWriteDefault);
		}

		if (consistencyLevelReadMap != null)
		{
			configMap.put(CONSISTENCY_LEVEL_READ_MAP_PARAM, consistencyLevelReadMap);
		}
		if (consistencyLevelWriteMap != null)
		{
			configMap.put(CONSISTENCY_LEVEL_WRITE_MAP_PARAM, consistencyLevelWriteMap);
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

	public void setConsistencyLevelReadDefault(String consistencyLevelReadDefault)
	{
		this.consistencyLevelReadDefault = consistencyLevelReadDefault;
	}

	public void setConsistencyLevelWriteDefault(String consistencyLevelWriteDefault)
	{
		this.consistencyLevelWriteDefault = consistencyLevelWriteDefault;
	}

	public void setConsistencyLevelReadMap(Map<String, String> consistencyLevelReadMap)
	{
		this.consistencyLevelReadMap = consistencyLevelReadMap;
	}

	public void setConsistencyLevelWriteMap(Map<String, String> consistencyLevelWriteMap)
	{
		this.consistencyLevelWriteMap = consistencyLevelWriteMap;
	}

	public void setEnsureJoinConsistency(boolean ensureJoinConsistency)
	{
		this.ensureJoinConsistency = ensureJoinConsistency;
	}

	public ThriftEntityManager getObject() throws Exception
	{
		return em;
	}

	@Override
	public Class<?> getObjectType()
	{
		return ThriftEntityManager.class;
	}

	@Override
	public boolean isSingleton()
	{
		return true;
	}

}
