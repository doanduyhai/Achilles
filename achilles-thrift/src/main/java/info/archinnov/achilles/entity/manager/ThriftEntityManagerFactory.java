package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.configuration.ThriftArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext.Impl;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftDaoContextBuilder;
import info.archinnov.achilles.table.ThriftTableCreator;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.Collections;
import java.util.Map;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftEntityManagerFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityManagerFactory extends EntityManagerFactory {

    private static final Logger log = LoggerFactory.getLogger(ThriftEntityManagerFactory.class);

    private Cluster cluster;
    private Keyspace keyspace;

    private ThriftDaoContext thriftDaoContext;

    /**
     * Create a new ThriftEntityManagerFactoryImpl with a configuration map
     * 
     * @param configurationMap
     *            Check documentation for more details on configuration parameters
     */
    public ThriftEntityManagerFactory(Map<String, Object> configurationMap) {
        super(configurationMap, new ThriftArgumentExtractor());
        configContext.setImpl(Impl.THRIFT);

        ThriftArgumentExtractor thriftArgumentExtractor = new ThriftArgumentExtractor();
        cluster = thriftArgumentExtractor.initCluster(configurationMap);
        keyspace = thriftArgumentExtractor.initKeyspace(cluster,
                (ThriftConsistencyLevelPolicy) configContext.getConsistencyPolicy(), configurationMap);

        log.info("Initializing Achilles ThriftEntityManagerFactory for cluster '{}' and keyspace '{}' ",
                cluster.getName(), keyspace.getKeyspaceName());

        boolean hasSimpleCounter = bootstrap();
        new ThriftTableCreator(cluster, keyspace).validateOrCreateTables(entityMetaMap, configContext,
                hasSimpleCounter);

        thriftDaoContext = new ThriftDaoContextBuilder().buildDao(cluster, keyspace, entityMetaMap, configContext,
                hasSimpleCounter);
    }

    /**
     * Create a new ThriftEntityManager. This instance of ThriftEntityManager is <strong>thread-safe</strong>
     * 
     * @return ThriftEntityManager
     */
    public ThriftEntityManager createEntityManager() {
        log.info("Create new Thrift-based Entity Manager ");

        return new ThriftEntityManager(this, Collections.unmodifiableMap(entityMetaMap), //
                thriftDaoContext, configContext);
    }

    @Override
    protected AchillesConsistencyLevelPolicy initConsistencyLevelPolicy(Map<String, Object> configurationMap,
            ArgumentExtractor argumentExtractor) {
        log.info("Initializing new Achilles Configurable Consistency Level Policy from arguments ");

        ConsistencyLevel defaultReadConsistencyLevel = argumentExtractor
                .initDefaultReadConsistencyLevel(configurationMap);
        ConsistencyLevel defaultWriteConsistencyLevel = argumentExtractor
                .initDefaultWriteConsistencyLevel(configurationMap);
        Map<String, ConsistencyLevel> readConsistencyMap = argumentExtractor.initReadConsistencyMap(configurationMap);
        Map<String, ConsistencyLevel> writeConsistencyMap = argumentExtractor
                .initWriteConsistencyMap(configurationMap);

        return new ThriftConsistencyLevelPolicy(defaultReadConsistencyLevel, defaultWriteConsistencyLevel,
                readConsistencyMap, writeConsistencyMap);
    }

    protected void setThriftDaoContext(ThriftDaoContext thriftDaoContext) {
        this.thriftDaoContext = thriftDaoContext;
    }

}
