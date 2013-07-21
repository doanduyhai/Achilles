package info.archinnov.achilles.common;

import static info.archinnov.achilles.configuration.ConfigurationParameters.ENSURE_CONSISTENCY_ON_JOIN_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_CF_CREATION_PARAM;
import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.CLUSTER_PARAM;
import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.KEYSPACE_PARAM;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;
import info.archinnov.achilles.type.Pair;
import java.util.Map;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.commons.lang.StringUtils;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;

/**
 * ThriftCassandraDaoTest
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class ThriftCassandraDaoTest extends AbstractCassandraDaoTest {

    private static final String ENTITY_PACKAGE = "info.archinnov.achilles.test.integration.entity";
    private static Cluster cluster;
    private static Keyspace keyspace;
    private static ThriftConsistencyLevelPolicy policy;

    public static final Logger log = LoggerFactory.getLogger(ThriftCassandraDaoTest.class);

    private static ThriftEntityManagerFactory emf;

    static {
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

        Map<String, Object> configMap = ImmutableMap.of(ENTITY_PACKAGES_PARAM, ENTITY_PACKAGE, CLUSTER_PARAM,
                getCluster(), KEYSPACE_PARAM, getKeyspace(), FORCE_CF_CREATION_PARAM, true,
                ENSURE_CONSISTENCY_ON_JOIN_PARAM, true);

        emf = new ThriftEntityManagerFactory(configMap);
        ConfigurationContext configContext = Whitebox.getInternalState(emf, "configContext");
        policy = (ThriftConsistencyLevelPolicy) configContext.getConsistencyPolicy();
    }

    public static Cluster getCluster() {
        return cluster;
    }

    public static Keyspace getKeyspace() {
        return keyspace;
    }

    public static ThriftEntityManager getEm() {
        return emf.createEntityManager();
    }

    public static <K> ThriftGenericEntityDao getEntityDao(String columnFamily, Class<K> keyClass) {
        return new ThriftGenericEntityDao(cluster, keyspace, columnFamily, policy, new Pair<Class<K>, Class<String>>(
                keyClass, String.class));
    }

    public static <K, V> ThriftGenericWideRowDao getColumnFamilyDao(String columnFamily, Class<K> keyClass,
            Class<V> valueClass) {

        return new ThriftGenericWideRowDao(cluster, keyspace, columnFamily, policy, new Pair<Class<K>, Class<V>>(
                keyClass, valueClass));
    }

    public static ThriftCounterDao getCounterDao() {
        Pair<Class<Composite>, Class<Long>> rowkeyAndValueClasses = new Pair<Class<Composite>, Class<Long>>(
                Composite.class, Long.class);
        return new ThriftCounterDao(cluster, keyspace, policy, rowkeyAndValueClasses);
    }

    public static ThriftConsistencyLevelPolicy getConsistencyPolicy() {
        return policy;
    }
}
