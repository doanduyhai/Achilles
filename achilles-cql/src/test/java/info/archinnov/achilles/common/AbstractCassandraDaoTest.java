package info.archinnov.achilles.common;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractCassandraDaoTest
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AbstractCassandraDaoTest
{
	private static final String CASSANDRA_TEST_YAML_CONFIG_TEMPLATE = "src/test/resources/cassandraUnitConfig.yaml";
	private static final String CASSANDRA_TEST_COLUMN_FAMILIES_CONFIG = "columnFamilies.json";
	private static final String CASSANDRA_TEST_CLUSTER_NAME = "Achilles Test Cassandra Cluster";
	protected static final String CASSANDRA_KEYSPACE_NAME = "achilles";
	protected static final String CASSANDRA_TEST_HOST = "localhost";
	protected static final String CASSANDRA_HOST = "cassandraHost";
	protected static int CASSANDRA_THRIFT_TEST_PORT = 9160;
	protected static int CASSANDRA_CQL_TEST_PORT = 9042;

	public static final Logger log = LoggerFactory.getLogger(AbstractCassandraDaoTest.class);

	static
	{
		String cassandraHost = System.getProperty(CASSANDRA_HOST);
		if (StringUtils.isBlank(cassandraHost))
		{
			try
			{
				final File temporaryCassandraYaml = prepareEmbeddedCassandraConfig();

				EmbeddedCassandraServerHelper.startEmbeddedCassandra(temporaryCassandraYaml,
						EmbeddedCassandraServerHelper.DEFAULT_TMP_DIR);
				Runtime.getRuntime().addShutdownHook(new Thread()
				{
					@Override
					public void run()
					{
						FileUtils.deleteQuietly(temporaryCassandraYaml);
						log.info("Shutting down cassandra");
					}
				});
			}
			catch (Exception e1)
			{
				throw new IllegalStateException("Cannot start cassandra embedded", e1);
			}
			DataLoader dataLoader = new DataLoader(CASSANDRA_TEST_CLUSTER_NAME, CASSANDRA_TEST_HOST
					+ ":" + CASSANDRA_THRIFT_TEST_PORT);
			dataLoader.load(new ClassPathJsonDataSet(CASSANDRA_TEST_COLUMN_FAMILIES_CONFIG));

		}

	}

	private static File prepareEmbeddedCassandraConfig() throws IOException
	{
		File cassandraYamlTemplate = new File(CASSANDRA_TEST_YAML_CONFIG_TEMPLATE);
		File temporaryCassandraYaml = File.createTempFile("testCassandra-", ".yaml");
		FileUtils.copyFile(cassandraYamlTemplate, temporaryCassandraYaml);

		log.info(" Temporary cassandra.yaml file = {}", temporaryCassandraYaml.getAbsolutePath());

		String storagePort = "storage_port: " + (7001 + RandomUtils.nextInt(990));
		CASSANDRA_THRIFT_TEST_PORT = 9500 + RandomUtils.nextInt(499);
		String rcpPort = "rpc_port: " + CASSANDRA_THRIFT_TEST_PORT;
		CASSANDRA_CQL_TEST_PORT = 9000 + RandomUtils.nextInt(499);
		String cqlPort = "native_transport_port: " + CASSANDRA_CQL_TEST_PORT;
		log.info(" Random embedded Cassandra RPC port = {}", CASSANDRA_THRIFT_TEST_PORT);
		log.info(" Random embedded Cassandra Native port = {}", CASSANDRA_CQL_TEST_PORT);
		FileUtils.writeLines(temporaryCassandraYaml, Arrays.asList(storagePort, rcpPort, cqlPort),
				true);

		return temporaryCassandraYaml;
	}
}
