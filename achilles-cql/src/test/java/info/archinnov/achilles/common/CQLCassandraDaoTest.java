package info.archinnov.achilles.common;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManagerFactory;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * CQLCassandraDaoTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLCassandraDaoTest extends AbstractCassandraDaoTest
{
	private static final String ENTITY_PACKAGE = "integration.tests.entity";

	private static Cluster cqlCluster;
	private static Session cqlSession;
	private static CQLEntityManagerFactory emf;

	static
	{
		String cassandraHost = System.getProperty(CASSANDRA_HOST);
		if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":"))
		{
			String[] fullHostName = StringUtils.split(cassandraHost, ":");

			assert fullHostName.length == 2;

			cqlCluster = Cluster.builder() //
					.addContactPoints(fullHostName[0])
					.withPort(Integer.parseInt(fullHostName[1]))
					.build();

		}
		else
		{
			cqlCluster = Cluster.builder() //
					.addContactPoints(CASSANDRA_TEST_HOST)
					.withPort(CASSANDRA_CQL_TEST_PORT)
					.build();
		}
		cqlSession = cqlCluster.connect(CASSANDRA_KEYSPACE_NAME);
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put(ENTITY_PACKAGES_PARAM, ENTITY_PACKAGE);
		configMap.put(CONNECTION_CONTACT_POINTS_PARAM, CASSANDRA_TEST_HOST);
		configMap.put(CONNECTION_PORT_PARAM, CASSANDRA_CQL_TEST_PORT + "");
		configMap.put(KEYSPACE_NAME_PARAM, CASSANDRA_KEYSPACE_NAME);
		configMap.put(FORCE_CF_CREATION_PARAM, true);
		configMap.put(ENSURE_CONSISTENCY_ON_JOIN_PARAM, true);

		emf = new CQLEntityManagerFactory(configMap);
	}

	public static com.datastax.driver.core.Cluster getCqlCluster()
	{
		return cqlCluster;
	}

	public static Session getCqlSession()
	{
		return cqlSession;
	}

	public static int getCqlPort()
	{
		return CASSANDRA_CQL_TEST_PORT;
	}

	public static CQLEntityManager getEm()
	{
		return emf.createEntityManager();
	}
}
