package info.archinnov.achilles.junit;

import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.embedded.ThriftEmbeddedServer;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;

/**
 * AchillesThriftResource
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesThriftResource extends AchillesTestResource {

    private final ThriftEmbeddedServer server;
    private final Cluster cluster;
    private final Keyspace keyspace;
    private final ThriftConsistencyLevelPolicy policy;

    private final ThriftEntityManagerFactory factory;
    private final ThriftEntityManager em;

    /**
     * Initialize a new embedded Cassandra server
     * 
     * @param entityPackages
     *            packages to scan for entity discovery, comma separated
     * 
     * @param tables
     *            list of tables to truncate before and after tests
     */
    public AchillesThriftResource(String entityPackages, String... tables) {
        super(tables);
        if (StringUtils.isEmpty(entityPackages))
            throw new IllegalArgumentException("Entity packages should be provided");

        server = new ThriftEmbeddedServer(entityPackages);
        cluster = server.getCluster();
        keyspace = server.getKeyspace();
        policy = server.getConsistencyPolicy();
        factory = server.getEmf();
        em = server.getEm();
    }

    /**
     * Initialize a new embedded Cassandra server
     * 
     * @param entityPackages
     *            packages to scan for entity discovery, comma separated
     * 
     * @param cleanUpSteps
     *            when to truncate tables for clean up. Possible values are : Steps.BEFORE_TEST, Steps.AFTER_TEST and
     *            Steps.BOTH (Default value) <br/>
     * <br/>
     * 
     * @param tables
     *            list of tables to truncate before, after or before and after tests, depending on the 'cleanUpSteps'
     *            parameters
     */
    public AchillesThriftResource(String entityPackages, Steps cleanUpSteps, String... tables) {
        super(cleanUpSteps, tables);
        if (StringUtils.isEmpty(entityPackages))
            throw new IllegalArgumentException("Entity packages should be provided");

        server = new ThriftEmbeddedServer(entityPackages);
        cluster = server.getCluster();
        keyspace = server.getKeyspace();
        policy = server.getConsistencyPolicy();
        factory = server.getEmf();
        em = server.getEm();
    }

    /**
     * Return the native Hector cluster
     * 
     * @return
     *         native Hector cluster
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * Return the native Hector keyspace
     * 
     * @return
     *         native Hector keyspace
     */
    public Keyspace getKeyspace() {
        return keyspace;
    }

    /**
     * Return a singleton ThriftEntityManagerFactory
     * 
     * @return
     *         ThriftEntityManagerFactory singleton
     */
    public ThriftEntityManagerFactory getFactory() {
        return factory;
    }

    /**
     * Return a singleton ThriftEntityManager
     * 
     * @return
     *         ThriftEntityManager singleton
     */
    public ThriftEntityManager getEm() {
        return em;
    }

    /**
     * Return a singleton ThriftConsistencyLevelPolicy
     * 
     * @return
     *         ThriftConsistencyLevelPolicy singleton
     */
    public ThriftConsistencyLevelPolicy getConsistencyPolicy() {
        return policy;
    }

    @Override
    protected void truncateTables() {
        if (tables != null)
        {
            for (String table : tables)
            {
                cluster.truncate(keyspace.getKeyspaceName(), table);
            }
        }
    }

    public <K> ThriftGenericEntityDao getEntityDao(String columnFamily, Class<K> keyClass) {
        return new ThriftGenericEntityDao(cluster, keyspace, columnFamily, policy, Pair.create(
                keyClass, String.class));
    }

    public <K, V> ThriftGenericWideRowDao getColumnFamilyDao(String columnFamily, Class<K> keyClass,
            Class<V> valueClass) {

        return new ThriftGenericWideRowDao(cluster, keyspace, columnFamily, policy, Pair.create(
                keyClass, valueClass));
    }

    public ThriftCounterDao getCounterDao() {
        Pair<Class<Composite>, Class<Long>> rowkeyAndValueClasses = Pair.create(
                Composite.class, Long.class);
        return new ThriftCounterDao(cluster, keyspace, policy, rowkeyAndValueClasses);
    }

}
