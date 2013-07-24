package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.KEYSPACE_NAME_PARAM;
import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.configuration.CQLArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.consistency.CQLConsistencyLevelPolicy;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLDaoContextBuilder;
import info.archinnov.achilles.context.ConfigurationContext.Impl;
import info.archinnov.achilles.table.CQLTableCreator;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * CQLEntityManagerFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityManagerFactory extends EntityManagerFactory {
    private static final Logger log = LoggerFactory.getLogger(CQLEntityManagerFactory.class);
    private Cluster cluster;
    private Session session;
    private CQLDaoContext daoContext;

    /**
     * Create a new CQLEntityManagerFactory with a configuration map
     * 
     * @param configurationMap
     *            Check documentation for more details on configuration parameters
     */
    public CQLEntityManagerFactory(Map<String, Object> configurationMap) {
        super(configurationMap, new CQLArgumentExtractor());
        configContext.setImpl(Impl.CQL);

        CQLArgumentExtractor extractor = new CQLArgumentExtractor();
        cluster = extractor.initCluster(configurationMap);
        session = extractor.initSession(cluster, configurationMap);

        boolean hasSimpleCounter = bootstrap();
        new CQLTableCreator(cluster, session, (String) configurationMap.get(KEYSPACE_NAME_PARAM))
                .validateOrCreateTables(entityMetaMap, configContext, hasSimpleCounter);

        daoContext = CQLDaoContextBuilder.builder(session).build(entityMetaMap, hasSimpleCounter);

    }

    /**
     * Create a new CQLEntityManager. This instance of CQLEntityManager is <strong>thread-safe</strong>
     * 
     * @return CQLEntityManager
     */
    public CQLEntityManager createEntityManager() {
        return new CQLEntityManager(entityMetaMap, configContext, daoContext);
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

        return new CQLConsistencyLevelPolicy(defaultReadConsistencyLevel, defaultWriteConsistencyLevel,
                readConsistencyMap, writeConsistencyMap);
    }

}
