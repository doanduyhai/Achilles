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
package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.embedded.ServerStarter.CASSANDRA_EMBEDDED;
import static info.archinnov.achilles.embedded.StateRepository.REPOSITORY;
import static info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper.ACHILLES_DML_STATEMENT;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import info.archinnov.achilles.internal.utils.ConfigMap;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.type.TypedMap;

public class CassandraEmbeddedServer {

    public static final Logger LOGGER = LoggerFactory.getLogger(CassandraEmbeddedServer.class);

    public static final String CASSANDRA_HOST = "cassandraHost";

    private static final Object SEMAPHORE = new Object();

    private static boolean embeddedServerStarted = false;

    private static final AchillesInitializer initializer = new AchillesInitializer();

    private static final Logger DML_LOGGER = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);

    /**
     * Start a Cassandra embedded server
     * <em>This constructor is not meant to be used directly. Please use the
     * {@code info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder} instead
     * </em>
     * @param parameters embedded Cassandra server parameters
     * @param achillesParameters Achilles parameters
     */
    public CassandraEmbeddedServer(TypedMap originalParameters, ConfigMap achillesParameters) {
        LOGGER.trace("Start Cassandra Embedded server with server and Achilles config");
        TypedMap parameters = CassandraEmbeddedConfigParameters.mergeWithDefaultParameters(originalParameters);
        String cassandraHost = System.getProperty(CASSANDRA_HOST);

        // No external Cassandra server, start an embedded instance
        if (StringUtils.isBlank(cassandraHost)) {
            synchronized (SEMAPHORE) {
                if (!embeddedServerStarted) {
                    CASSANDRA_EMBEDDED.startServer(cassandraHost, parameters);
                    CassandraEmbeddedServer.embeddedServerStarted = true;
                } else {
                    CASSANDRA_EMBEDDED.checkAndConfigurePorts(parameters);
                }
            }
        }
        initializer.initializeFromParameters(cassandraHost, parameters, achillesParameters);
    }

    public PersistenceManagerFactory getPersistenceManagerFactory(String keyspaceName) {
        return REPOSITORY.getManagerFactoryForKeyspace(keyspaceName);
    }

    public PersistenceManager getPersistenceManager(String keyspaceName) {
        return REPOSITORY.getManagerForKeyspace(keyspaceName);
    }

    public Session getNativeSession(String keyspaceName) {
        return REPOSITORY.getSessionForKeyspace(keyspaceName);
    }

    public static int getThriftPort() {
        return CASSANDRA_EMBEDDED.getThriftPort();
    }

    public static int getCqlPort() {
        return CASSANDRA_EMBEDDED.getCQLPort();
    }

    public void truncateTable(String keyspaceName, String tableName) {
        String query = "TRUNCATE " + tableName;
        Session session = REPOSITORY.getSessionForKeyspace(keyspaceName);
        session.execute(new SimpleStatement(query).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL));
        DML_LOGGER.debug("{} : [{}] with CONSISTENCY LEVEL [{}]", "  Simple query", query, "ALL");
    }

}
