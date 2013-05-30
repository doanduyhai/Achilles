package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.Policies;

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
		String contactPoints = (String) configurationMap.get(CONNECTION_CONTACT_POINTS_PARAM);
		String port = (String) configurationMap.get(CONNECTION_PORT_PARAM);
		Validator.validateNotBlank(contactPoints, CONNECTION_CONTACT_POINTS_PARAM
				+ " property should be provided");
		Validator.validateNotBlank(port, CONNECTION_PORT_PARAM + " property should be provided");
		Validator.validateTrue(NumberUtils.isNumber(port), CONNECTION_PORT_PARAM
				+ " property should be a number");

		String[] contactPointsList = StringUtils.split(contactPoints, ",");

		Cluster cluster = Cluster.builder() //
				.addContactPoints(contactPointsList)
				.withPort(Integer.parseInt(port))
				.withCompression(Compression.SNAPPY)
				.withRetryPolicy(Policies.DEFAULT_RETRY_POLICY)
				.withLoadBalancingPolicy(Policies.DEFAULT_LOAD_BALANCING_POLICY)
				.withReconnectionPolicy(Policies.DEFAULT_RECONNECTION_POLICY)
				.build();

		return cluster;

	}

	public Session initSession(Cluster cluster, Map<String, Object> configurationMap)
	{
		String keyspace = (String) configurationMap.get(KEYSPACE_NAME_PARAM);
		Validator.validateNotBlank(keyspace, KEYSPACE_NAME_PARAM + " property should be provided");

		return cluster.connect(keyspace);
	}

}
