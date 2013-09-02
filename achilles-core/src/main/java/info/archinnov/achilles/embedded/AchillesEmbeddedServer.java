package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.embedded.CassandraEmbedded.CASSANDRA_EMBEDDED;
import java.io.File;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AchillesEmbeddedServer {

    public static final String CASSANDRA_TEST_KEYSPACE_NAME = "achilles_test";
    protected static final String CASSANDRA_EMBEDDED_HOME = "target/cassandra_embedded";
    protected static final String CASSANDRA_TEST_HOST = "localhost";
    protected static final String CASSANDRA_HOST = "cassandraHost";
    protected static int CASSANDRA_THRIFT_TEST_PORT = 9160;
    protected static int CASSANDRA_CQL_TEST_PORT = 9042;
    private static final String CASSANDRA_TEST_CLUSTER_NAME = "Achilles Test Cassandra Cluster";

    public static final Logger log = LoggerFactory.getLogger(AchillesEmbeddedServer.class);

    protected void startServer(boolean cleanCassandraDataFile) {
        if (cleanCassandraDataFile)
        {
            CASSANDRA_EMBEDDED.cleanCassandraDataFiles(CASSANDRA_EMBEDDED_HOME);
        }

        String cassandraHost = System.getProperty(CASSANDRA_HOST);
        if (StringUtils.isBlank(cassandraHost))
        {
            log.info(" Embedded Cassandra home = ./{}", CASSANDRA_EMBEDDED_HOME);
            CassandraConfig cassandraConfig = new CassandraConfig(CASSANDRA_TEST_CLUSTER_NAME, new File(
                    CASSANDRA_EMBEDDED_HOME));
            cassandraConfig.randomPorts();
            CASSANDRA_CQL_TEST_PORT = cassandraConfig.getCqlPort();
            CASSANDRA_THRIFT_TEST_PORT = cassandraConfig.getRPCPort();

            log.info(" Random embedded Cassandra RPC port = {}", CASSANDRA_THRIFT_TEST_PORT);
            log.info(" Random embedded Cassandra Native port = {}", CASSANDRA_CQL_TEST_PORT);

            //Start embedded server
            CASSANDRA_EMBEDDED.start(cassandraConfig);
        }
    }
}
