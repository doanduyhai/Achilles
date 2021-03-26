/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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
package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.embedded.ServerStarter.CASSANDRA_EMBEDDED;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.type.TypedMap;

public class CassandraEmbeddedServer {

    public static final Logger LOGGER = LoggerFactory.getLogger(CassandraEmbeddedServer.class);

    public static final String CASSANDRA_HOST = "cassandraHost";

    static final Object SEMAPHORE = new Object();
    static boolean embeddedServerStarted = false;
    private static final AchillesInitializer initializer = new AchillesInitializer();


    /**
     * Start a Cassandra embedded server
     * <em>This constructor is not meant to be used directly. Please use the
     * {@code info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder} instead
     * </em>
     *
     * @param originalParameters embedded Cassandra server parameters
     */
    public CassandraEmbeddedServer(TypedMap originalParameters) {
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
        initializer.initializeFromParameters(cassandraHost, parameters);
    }


    public Cluster getNativeCluster() {
        return initializer.getSingletonCluster();
    }

    public Session getNativeSession() {
        return initializer.getSingletonSession();
    }

    public void registerSessionForShutdown(Session session) {
        ServerStarter.CASSANDRA_EMBEDDED.getShutdownHook().addSession(session);
    }
}
