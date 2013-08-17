package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;
import java.util.Map;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.commons.lang.StringUtils;
import com.google.common.collect.ImmutableMap;

/**
 * ThriftEmbeddedServer
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEmbeddedServer extends AbstractEmbeddedServer {

    private static final Object SEMAPHORE = new Object();

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
            keyspace = HFactory.createKeyspace(CASSANDRA_KEYSPACE_NAME, cluster);
        } else {
            cluster = HFactory.getOrCreateCluster("Achilles-cluster", CASSANDRA_TEST_HOST + ":"
                    + CASSANDRA_THRIFT_TEST_PORT);
            keyspace = HFactory.createKeyspace(CASSANDRA_KEYSPACE_NAME, cluster);
        }

        Map<String, Object> configMap = ImmutableMap.of(ENTITY_PACKAGES_PARAM, entityPackages, CLUSTER_PARAM,
                cluster, KEYSPACE_PARAM, getKeyspace(), FORCE_CF_CREATION_PARAM, true,
                ENSURE_CONSISTENCY_ON_JOIN_PARAM, true);

        emf = new ThriftEntityManagerFactory(configMap);
        em = emf.createEntityManager();
        policy = emf.getConsistencyPolicy();
        initialized = true;
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
