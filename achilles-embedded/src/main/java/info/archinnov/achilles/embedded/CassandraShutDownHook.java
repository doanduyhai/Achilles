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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shutdown hook to control when to shutdown the embedded Cassandra process.
 * <br/>
 * <pre class="code"><code class="java">
 *
 * CassandraShutDownHook shutdownHook = new CassandraShutDownHook();
 *
 * Session session = CassandraEmbeddedServerBuilder.builder()
 *   .withShutdownHook(shutdownHook)
 *   ...
 *   .buildNativeSession();
 *
 * ...
 *
 * shutdownHook.shutdownNow();
 * </code></pre>
 * <br/>
 * <strong>Please note that upon call on <em>shutdownNow()</em>, Achilles will trigger the shutdown of:</strong>
 * <ul>
 *     <li><strong>the embedded Cassandra server</strong></li>
 *     <li><strong>the associated Cluster object</strong></li>
 *     <li><strong>the associated Session object</strong></li>
 * </ul>
 */
public class CassandraShutDownHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraShutDownHook.class);

    private AtomicReference<CassandraDaemon> cassandraDaemonRef;
    private OrderedShutdownHook orderedShutdownHook;
    private ExecutorService executor;

    void addCassandraDaemonRef(AtomicReference<CassandraDaemon> cassandraDaemonRef) {
        this.cassandraDaemonRef = cassandraDaemonRef;
    }

    void addOrderedShutdownHook(OrderedShutdownHook orderedShutdownHook) {
        this.orderedShutdownHook = orderedShutdownHook;
    }

    void addExecutorService(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Trigger the shutdown of:
     * <ul>
     *     <li><strong>the embedded Cassandra server</strong></li>
     *     <li><strong>the associated Cluster object</strong></li>
     *     <li><strong>the associated Session object</strong></li>
     * </ul>
     */
    public void shutDownNow() throws InterruptedException {
        synchronized (CassandraEmbeddedServer.SEMAPHORE) {
            LOGGER.info("Calling stop on Embedded Cassandra server");
            cassandraDaemonRef.get().stop();

            LOGGER.info("Calling shutdown on all Cluster instances");
            // First call shutdown on all registered Java driver Cluster instances
            orderedShutdownHook.callShutDown();

            LOGGER.info("Shutting down embedded Cassandra server");
            // Then shutdown the server
            executor.shutdownNow();
            CassandraEmbeddedServer.embeddedServerStarted = false;
        }
    }
}
