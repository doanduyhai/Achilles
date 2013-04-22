package info.archinnov.achilles.common;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Session;

/**
 * CQLCassandraDaoTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLCassandraDaoTest extends AbstractCassandraDaoTest
{
	private static com.datastax.driver.core.Cluster cqlCluster;
	private static com.datastax.driver.core.Session cqlSession;

	static
	{
		String cassandraHost = System.getProperty(CASSANDRA_HOST);
		if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":"))
		{
			String[] fullHostName = StringUtils.split(cassandraHost, ":");

			assert fullHostName.length == 2;

			cqlCluster = com.datastax.driver.core.Cluster.builder() //
					.addContactPoints(fullHostName[0])//
					.withPort(Integer.parseInt(fullHostName[1])) //
					.build();

		}
		else
		{
			cqlCluster = com.datastax.driver.core.Cluster.builder() //
					.addContactPoints(CASSANDRA_TEST_HOST)//
					.withPort(CASSANDRA_CQL_TEST_PORT) //
					.build();
		}
		cqlSession = cqlCluster.connect(CASSANDRA_KEYSPACE_NAME);

	}

	public static com.datastax.driver.core.Cluster getCqlCluster()
	{
		return cqlCluster;
	}

	public static Session getCqlSession()
	{
		return cqlSession;
	}
}
