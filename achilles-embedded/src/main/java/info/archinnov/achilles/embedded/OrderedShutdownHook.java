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

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class OrderedShutdownHook {

    private static final Logger log = LoggerFactory.getLogger(OrderedShutdownHook.class);

    private final Queue<Cluster> clusters = new ConcurrentLinkedQueue<>();
    private final Set<Session> sessions = new CopyOnWriteArraySet<>();

    void addCluster(Cluster cluster) {
        clusters.add(cluster);
    }

    void addSession(Session session) {
        if (!sessions.contains(session)) {
            sessions.add(session);
        }

    }


    void callShutDown() {
        sessions.forEach(session -> {
            log.info(String.format("Call shutdown on Session instance '%s'", session.toString()));
            session.close();
        });

        clusters.forEach(cluster -> {
            log.info(String.format("Call shutdown on Cluster instance '%s' of cluster name '%s'", cluster, cluster.getClusterName()));
            try {
                cluster.close();
            } catch (Throwable throwable) {

            }
        });
    }
}
