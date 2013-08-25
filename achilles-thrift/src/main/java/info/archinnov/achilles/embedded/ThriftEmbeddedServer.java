package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;
import java.util.ArrayList;
import java.util.Map;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;

/**
 * ThriftEmbeddedServer
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEmbeddedServer extends AchillesEmbeddedServer {

    private static final Object SEMAPHORE = new Object();
    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftEmbeddedServer.class);

    private static String entityPackages;
    private static boolean initialized = false;
    private static Cluster cluster;
    private static Keyspace keyspace;
    private static ThriftConsistencyLevelPolicy policy;

    private static ThriftEntityManagerFactory emf;
    private static ThriftEntityManager em;

    public ThriftEmbeddedServer(String entityPackages) {
        if (StringUtils.isEmpty(entityPackages))
            throw new IllegalArgumentException("Entity packages should be provided");

        synchronized (SEMAPHORE) {
            if (!initialized)
            {
                ThriftEmbeddedServer.entityPackages = entityPackages;
                initialize();
            }
        }
    }

    private void initialize() {
        String cassandraHost = System.getProperty(CASSANDRA_HOST);
        if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":")) {
            CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator(cassandraHost);
            cluster = HFactory.getOrCreateCluster("achilles", hostConfigurator);
            keyspace = HFactory.createKeyspace(CASSANDRA_TEST_KEYSPACE_NAME, cluster);
        } else {
            createAchillesKeyspace();
            cluster = HFactory.getOrCreateCluster("Achilles-cluster", CASSANDRA_TEST_HOST + ":"
                    + CASSANDRA_THRIFT_TEST_PORT);
            keyspace = HFactory.createKeyspace(CASSANDRA_TEST_KEYSPACE_NAME, cluster);
        }

        Map<String, Object> configMap = ImmutableMap.of(ENTITY_PACKAGES_PARAM, entityPackages, CLUSTER_PARAM,
                cluster, KEYSPACE_PARAM, getKeyspace(), FORCE_CF_CREATION_PARAM, true,
                ENSURE_CONSISTENCY_ON_JOIN_PARAM, true);

        emf = new ThriftEntityManagerFactory(configMap);
        em = emf.createEntityManager();
        policy = emf.getConsistencyPolicy();
        initialized = true;
    }

    private void createAchillesKeyspace() {

        TTransport tr = new TFramedTransport(new TSocket("localhost", CASSANDRA_THRIFT_TEST_PORT));
        TProtocol proto = new TBinaryProtocol(tr, true, true);
        Cassandra.Client client = new Cassandra.Client(proto);
        try {
            tr.open();

            for (KsDef ksDef : client.describe_keyspaces())
            {
                if (StringUtils.equals(ksDef.getName(), CASSANDRA_TEST_KEYSPACE_NAME))
                {
                    return;
                }
            }
            LOGGER.info("Create keyspace " + CASSANDRA_TEST_KEYSPACE_NAME);

            KsDef ksDef = new KsDef();
            ksDef.name = CASSANDRA_TEST_KEYSPACE_NAME;
            ksDef.replication_factor = 1;
            ksDef.strategy_options = ImmutableMap.of("replication_factor", "1");
            ksDef.strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
            ksDef.setCf_defs(new ArrayList<CfDef>());

            client.system_add_keyspace(ksDef);
        } catch (Exception e) {
            e.printStackTrace();
        } finally
        {
            tr.close();
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public ThriftEntityManagerFactory getEmf()
    {
        return emf;
    }

    public ThriftEntityManager getEm() {
        return em;
    }

    public ThriftConsistencyLevelPolicy getConsistencyPolicy() {
        return policy;
    }

    public static ThriftConsistencyLevelPolicy policy() {
        return policy;
    }
}
