package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static org.apache.commons.lang.StringUtils.contains;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * CQLArgumentExtractor
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLArgumentExtractor extends AchillesArgumentExtractor
{

	public Cluster initCluster(Map<String, Object> configurationMap)
	{
		String cassandraHost = (String) configurationMap.get(HOSTNAME_PARAM);
		Validator.validateNotBlank(cassandraHost, HOSTNAME_PARAM + " property should be provided");
		Validator.validateTrue(contains(cassandraHost, ":"), "Cassandra hostname property "
				+ cassandraHost + " should provide a port. Use : as separator");
		String[] fullHostName = StringUtils.split(cassandraHost, ":");

		Validator.validateTrue(fullHostName.length == 2, "Cassandra hostname property "
				+ cassandraHost + " should contain at most one : separator");

		return Cluster.builder() //
				.addContactPoints(fullHostName[0])
				.withPort(Integer.parseInt(fullHostName[1]))
				.build();
	}

	public Session initSession(Cluster cluster, Map<String, Object> configurationMap)
	{
		String keyspace = (String) configurationMap.get(KEYSPACE_NAME_PARAM);
		Validator.validateNotBlank(keyspace, KEYSPACE_NAME_PARAM + " property should be provided");

		return cluster.connect(keyspace);
	}

}
