package info.archinnov.achilles.junit;

import info.archinnov.achilles.embedded.CQLEmbeddedServer;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManagerFactory;
import info.archinnov.achilles.junit.AchillesTestResource;
import com.datastax.driver.core.Session;

/**
 * AchillesInternalCQLResource
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesInternalCQLResource extends AchillesTestResource {

    private static final String ENTITY_PACKAGES = "info.archinnov.achilles.test.integration.entity";

    private final CQLEntityManagerFactory factory;
    private final CQLEntityManager em;
    private final CQLEmbeddedServer server;
    private final Session session;

    /**
     * Initialize a new embedded Cassandra server
     * 
     * @param tables
     *            list of tables to truncate before and after tests
     */
    public AchillesInternalCQLResource(String... tables) {
        super(tables);

        server = new CQLEmbeddedServer(ENTITY_PACKAGES);
        factory = server.getEmf();
        em = server.getEm();
        session = em.getNativeSession();
    }

    /**
     * Initialize a new embedded Cassandra server
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
    public AchillesInternalCQLResource(Steps cleanUpSteps, String... tables) {
        super(cleanUpSteps, tables);

        server = new CQLEmbeddedServer(ENTITY_PACKAGES);
        factory = server.getEmf();
        em = server.getEm();
        session = em.getNativeSession();
    }

    /**
     * Return a singleton CQLEntityManagerFactory
     * 
     * @return
     *         CQLEntityManagerFactory singleton
     */
    public CQLEntityManagerFactory getFactory() {
        return factory;
    }

    /**
     * Return a singleton CQLEntityManager
     * 
     * @return
     *         CQLEntityManager singleton
     */
    public CQLEntityManager getEm() {
        return em;
    }

    /**
     * Return a native CQL3 Session
     * 
     * @return
     *         native CQL3 Session
     */
    public Session getNativeSession()
    {
        return session;
    }

    protected void truncateTables() {
        if (tables != null)
        {
            for (String table : tables)
            {
                server.truncateTable(table);
            }
        }
    }

}
