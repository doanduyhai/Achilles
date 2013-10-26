/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.test.integration;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_DATA_FILES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_ACHILLES_TEST_KEYSPACE_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_DURABLE_WRITE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_NAME;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.embedded.CQLEmbeddedServer;
import info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.entity.manager.CQLPersistenceManagerFactory;
import info.archinnov.achilles.junit.AchillesTestResource;

public class AchillesInternalCQLResource extends AchillesTestResource {

    private static final String ACHILLES_ENTITY_PACKAGES = "info.archinnov.achilles.test.integration.entity";

    private final CQLPersistenceManagerFactory pmf;

    private final CQLPersistenceManager manager;

    private final CQLEmbeddedServer server;

    private final Session session;

    /**
     * Initialize a new embedded Cassandra server
     *
     * @param tables list of tables to truncate before and after tests
     */
    public AchillesInternalCQLResource(String... tables) {
        super(tables);
        final ImmutableMap<String, Object> config = ImmutableMap
                .<String, Object>of(CLEAN_CASSANDRA_DATA_FILES, true, ENTITY_PACKAGES, ACHILLES_ENTITY_PACKAGES,
                                    KEYSPACE_NAME, DEFAULT_ACHILLES_TEST_KEYSPACE_NAME,KEYSPACE_DURABLE_WRITE,false);

        server = new CQLEmbeddedServer(config);
        pmf = server.getPersistenceManagerFactory();
        manager = server.getPersistenceManager();
        session = manager.getNativeSession();
    }

    /**
     * Initialize a new embedded Cassandra server
     *
     * @param cleanUpSteps when to truncate tables for clean up. Possible values are :
     *                     Steps.BEFORE_TEST, Steps.AFTER_TEST and Steps.BOTH (Default
     *                     value) <br/>
     *                     <br/>
     * @param tables       list of tables to truncate before, after or before and after
     *                     tests, depending on the 'cleanUpSteps' parameters
     */
    public AchillesInternalCQLResource(Steps cleanUpSteps, String... tables) {
        super(cleanUpSteps, tables);
        final ImmutableMap<String, Object> config = ImmutableMap
                .<String, Object>of(CLEAN_CASSANDRA_DATA_FILES, true, ENTITY_PACKAGES, ACHILLES_ENTITY_PACKAGES,
                                    KEYSPACE_NAME, DEFAULT_ACHILLES_TEST_KEYSPACE_NAME,KEYSPACE_DURABLE_WRITE,false);

        server = new CQLEmbeddedServer(config);
        pmf = server.getPersistenceManagerFactory();
        manager = server.getPersistenceManager();
        session = manager.getNativeSession();
    }

    /**
     * Return a singleton CQLPersistenceManagerFactory
     *
     * @return CQLPersistenceManagerFactory singleton
     */
    public CQLPersistenceManagerFactory getPersistenceManagerFactory() {
        return pmf;
    }

    /**
     * Return a singleton CQLPersistenceManager
     *
     * @return CQLPersistenceManager singleton
     */
    public CQLPersistenceManager getPersistenceManager() {
        return manager;
    }

    /**
     * Return a native CQL3 Session
     *
     * @return native CQL3 Session
     */
    public Session getNativeSession() {
        return session;
    }

    @Override
    protected void truncateTables() {
        if (tables != null) {
            for (String table : tables) {
                server.truncateTable(table);
            }
        }
    }

}
