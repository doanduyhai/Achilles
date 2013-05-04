package info.archinnov.achilles.common;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericWideRowDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.context.ConfigurationContext;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;

import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.commons.lang.StringUtils;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * CassandraDaoTest
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class ThriftCassandraDaoTest extends AbstractCassandraDaoTest
{

	private static final String ENTITY_PACKAGE = "integration.tests.entity";
	private static Cluster cluster;
	private static Keyspace keyspace;
	private static AchillesConfigurableConsistencyLevelPolicy policy;

	public static final Logger log = LoggerFactory.getLogger(ThriftCassandraDaoTest.class);

	private static ThriftEntityManagerFactory emf;

	static
	{
		String cassandraHost = System.getProperty(CASSANDRA_HOST);
		if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":"))
		{
			CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator(
					cassandraHost);
			cluster = HFactory.getOrCreateCluster("achilles", hostConfigurator);
			keyspace = HFactory.createKeyspace(CASSANDRA_KEYSPACE_NAME, cluster);
		}
		else
		{
			cluster = HFactory.getOrCreateCluster("Achilles-cluster", CASSANDRA_TEST_HOST + ":"
					+ CASSANDRA_THRIFT_TEST_PORT);
			keyspace = HFactory.createKeyspace(CASSANDRA_KEYSPACE_NAME, cluster);
		}

		Map<String, Object> configMap = ImmutableMap.of(ENTITY_PACKAGES_PARAM, ENTITY_PACKAGE,
				CLUSTER_PARAM, getCluster(), KEYSPACE_PARAM, getKeyspace(),
				FORCE_CF_CREATION_PARAM, true, ENSURE_CONSISTENCY_ON_JOIN_PARAM, true);

		emf = new ThriftEntityManagerFactory(configMap);
		ConfigurationContext configContext = Whitebox.getInternalState(emf, "configContext");
		policy = configContext.getConsistencyPolicy();
	}

	public static Cluster getCluster()
	{
		return cluster;
	}

	public static Keyspace getKeyspace()
	{
		return keyspace;
	}

	public static ThriftEntityManager getEm()
	{
		return (ThriftEntityManager) emf.createEntityManager();
	}

	public static <K> GenericEntityDao<K> getEntityDao(Serializer<K> keySerializer,
			String columnFamily)
	{
		return new GenericEntityDao<K>(cluster, keyspace, keySerializer, columnFamily, policy);
	}

	public static <K, V> GenericWideRowDao<K, V> getColumnFamilyDao(
			Serializer<K> keySerializer, Serializer<V> valueSerializer, String columnFamily)
	{
		return new GenericWideRowDao<K, V>(cluster, keyspace, keySerializer, valueSerializer,
				columnFamily, policy);
	}

	public static CounterDao getCounterDao()
	{
		return new CounterDao(cluster, keyspace, policy);
	}

	public static AchillesConfigurableConsistencyLevelPolicy getConsistencyPolicy()
	{
		return policy;
	}
}
