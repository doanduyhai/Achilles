package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * ThriftArgumentExtractor
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftArgumentExtractor extends AchillesArgumentExtractor
{

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

	public Keyspace initKeyspace(Cluster cluster, ThriftConsistencyLevelPolicy consistencyPolicy,
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
}
