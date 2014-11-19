package info.archinnov.achilles.embedded;

import com.datastax.driver.core.Cluster;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class OrderedShutdownHook {

    private static final Logger log = LoggerFactory.getLogger(OrderedShutdownHook.class);

    private final ConcurrentLinkedQueue<Cluster> clusters = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<PersistenceManagerFactory> persistenceManagerFactories = new ConcurrentLinkedQueue<>();

    void addCluster(Cluster cluster) {
        clusters.add(cluster);
    }

    void addManagerFactory(PersistenceManagerFactory factory) {
        persistenceManagerFactories.add(factory);
    }

    void callShutDown() {
        for (PersistenceManagerFactory factory : persistenceManagerFactories) {
            log.info("Call shutdown on "+factory);
            factory.shutDown();
        }
        for (Cluster cluster : clusters) {
            log.info(String.format("Call shutdown on Cluster instance '%s' of cluster name '%s'",cluster, cluster.getClusterName()));
            cluster.closeAsync().force();
        }
    }
}
