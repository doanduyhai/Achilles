package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.context.CQLDaoContext.ACHILLES_DML_STATEMENT;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManagerFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

/**
 * CQLEmbeddedServer
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEmbeddedServer extends AbstractEmbeddedServer {
    private static final Object SEMAPHORE = new Object();
    private static final Logger logger = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);

    private static String entityPackages;
    private static boolean initialized = false;

    private static Session session;
    private static CQLEntityManagerFactory emf;
    private static CQLEntityManager em;

    public CQLEmbeddedServer(String entityPackages) {
        if (StringUtils.isEmpty(entityPackages))
            throw new IllegalArgumentException("Entity packages should be provided");

        synchronized (SEMAPHORE) {
            if (!initialized)
            {
                CQLEmbeddedServer.entityPackages = entityPackages;
                initialize();
            }
        }
    }

    private void initialize() {

        Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(ENTITY_PACKAGES_PARAM, entityPackages);
        configMap.put(CONNECTION_CONTACT_POINTS_PARAM, CASSANDRA_TEST_HOST);
        configMap.put(CONNECTION_PORT_PARAM, CASSANDRA_CQL_TEST_PORT);
        configMap.put(KEYSPACE_NAME_PARAM, CASSANDRA_KEYSPACE_NAME);
        configMap.put(FORCE_CF_CREATION_PARAM, true);
        configMap.put(ENSURE_CONSISTENCY_ON_JOIN_PARAM, true);

        emf = new CQLEntityManagerFactory(configMap);
        em = emf.createEntityManager();
        session = em.getNativeSession();
        initialized = true;
    }

    public int getCqlPort() {
        return CASSANDRA_CQL_TEST_PORT;
    }

    public CQLEntityManagerFactory getEmf() {
        return emf;
    }

    public CQLEntityManager getEm() {
        return em;
    }

    public void truncateTable(String tableName) {
        String query = "truncate " + tableName;
        session.execute(new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.ALL));
        logger.debug("{} : [{}] with CONSISTENCY LEVEL [{}]", "Simple query", query, "ALL");
    }
}
