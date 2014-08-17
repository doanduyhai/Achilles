package info.archinnov.achilles.embedded;

import com.datastax.driver.core.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class OrderedShutdownHook {

    private static final Logger log = LoggerFactory.getLogger(OrderedShutdownHook.class);

    private final ConcurrentLinkedQueue<Cluster> clusters = new ConcurrentLinkedQueue<>();

    void addCluster(Cluster cluster) {
        clusters.add(cluster);
    }

    void callShutDown() {
        for (Cluster cluster : clusters) {
            log.info("Call shutdown on Cluster instance : "+cluster);
            cluster.closeAsync().force();
        }
    }
}
