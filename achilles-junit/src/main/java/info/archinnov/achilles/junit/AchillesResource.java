/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.junit;

import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_DATA_FILES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_ACHILLES_TEST_KEYSPACE_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_DURABLE_WRITE;

import com.datastax.driver.core.Session;
import info.archinnov.achilles.embedded.CassandraEmbeddedServer;
import info.archinnov.achilles.internal.utils.ConfigMap;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.type.TypedMap;

public class AchillesResource extends AchillesTestResource {

    private PersistenceManagerFactory pmf;

    private PersistenceManager manager;

    private CassandraEmbeddedServer server;

    private Session session;

    private String keyspaceToUse;

    AchillesResource(ConfigMap configMap, String... tables) {
        super(tables);
        initResource(configMap);
    }

    AchillesResource(ConfigMap configMap, Steps cleanUpSteps, String... tables) {
        super(cleanUpSteps, tables);
        initResource(configMap);
    }

    private void initResource(ConfigMap achillesConfig) {
        TypedMap cassandraConfig = buildConfigMap();
        buildAchillesConfigMap(achillesConfig);

        server = new CassandraEmbeddedServer(cassandraConfig, achillesConfig);
        pmf = server.getPersistenceManagerFactory(keyspaceToUse);
        manager = server.getPersistenceManager(keyspaceToUse);
        session = server.getNativeSession(keyspaceToUse);
    }

    private TypedMap buildConfigMap() {
        TypedMap config = new TypedMap();
        config.put(CLEAN_CASSANDRA_DATA_FILES, true);
        config.put(KEYSPACE_DURABLE_WRITE, false);
        return config;
    }

    private void buildAchillesConfigMap(ConfigMap configMap) {
        keyspaceToUse = configMap.getTypedOr(KEYSPACE_NAME,DEFAULT_ACHILLES_TEST_KEYSPACE_NAME);
        configMap.put(FORCE_TABLE_CREATION, true);
        configMap.put(KEYSPACE_NAME, keyspaceToUse);
    }


    /**
     * Return a singleton PersistenceManagerFactory
     *
     * @return PersistenceManagerFactory singleton
     */
    public PersistenceManagerFactory getPersistenceManagerFactory() {
        return pmf;
    }

    /**
     * Return a singleton PersistenceManager
     *
     * @return PersistenceManager singleton
     */
    public PersistenceManager getPersistenceManager() {
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
                server.truncateTable(keyspaceToUse, table);
            }
        }
    }

}
