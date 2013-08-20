/**
 *
 * Modified version of original class from HouseScream
 * 
 * https://github.com/housecream/server/blob/develop/server/ws/src/main/java/org/housecream/server/application/CassandraConfig.java
 * 
 */

package info.archinnov.achilles.embedded;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.Config.InternodeCompression;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.config.SeedProviderDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Loader;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;

public class CassandraConfig {

    private final Logger log = LoggerFactory.getLogger(CassandraConfig.class);
    private final File cassandraHome;
    private final File configFile;

    private Config config = new Config();

    public CassandraConfig(String clusterName, File cassandraHome) {
        this.cassandraHome = cassandraHome;
        this.configFile = new File(cassandraHome, "config.yaml");

        config.cluster_name = clusterName;
        //        config.initial_token = "";
        config.hinted_handoff_enabled = true;
        config.max_hint_window_in_ms = 10800000; // 3 hours
        config.hinted_handoff_throttle_in_kb = 1024;
        config.max_hints_delivery_threads = 2;
        config.authenticator = org.apache.cassandra.auth.AllowAllAuthenticator.class.getName();
        config.authorizer = org.apache.cassandra.auth.AllowAllAuthorizer.class.getName();
        config.permissions_validity_in_ms = 2000;
        config.partitioner = org.apache.cassandra.dht.Murmur3Partitioner.class.getName();
        config.disk_failure_policy = DiskFailurePolicy.stop;
        //        config.key_cache_size_in_mb ="";
        config.key_cache_save_period = 14400;
        config.row_cache_size_in_mb = 0;
        config.row_cache_save_period = 0;
        config.row_cache_provider = "SerializingCacheProvider";
        config.commitlog_sync = CommitLogSync.periodic;
        config.commitlog_sync_period_in_ms = 10000;
        config.commitlog_segment_size_in_mb = 32;
        config.flush_largest_memtables_at = 0.75;
        config.reduce_cache_sizes_at = 0.85;
        config.reduce_cache_capacity_to = 0.6;
        config.concurrent_reads = 32;
        config.concurrent_writes = 32;
        config.memtable_flush_queue_size = 4;
        config.trickle_fsync = false;
        config.trickle_fsync_interval_in_kb = 10240;
        config.storage_port = 7000;
        config.ssl_storage_port = 7001;
        config.listen_address = "127.0.0.1";
        config.start_native_transport = true;
        config.native_transport_port = 9042;
        config.start_rpc = true;
        config.rpc_address = "localhost";
        config.rpc_keepalive = true;
        config.rpc_server_type = "sync";
        config.thrift_framed_transport_size_in_mb = 15;
        config.incremental_backups = false;
        config.snapshot_before_compaction = false;
        config.auto_snapshot = true;
        config.column_index_size_in_kb = 64;
        config.in_memory_compaction_limit_in_mb = 64;
        config.multithreaded_compaction = false;
        config.compaction_throughput_mb_per_sec = 16;
        config.compaction_preheat_key_cache = true;
        config.read_request_timeout_in_ms = 10000L;
        config.range_request_timeout_in_ms = 10000L;
        config.write_request_timeout_in_ms = 10000L;
        config.truncate_request_timeout_in_ms = 60000L;
        config.request_timeout_in_ms = 10000L;
        config.cross_node_timeout = false;
        config.endpoint_snitch = "SimpleSnitch";
        config.dynamic_snitch_update_interval_in_ms = 100;
        config.dynamic_snitch_reset_interval_in_ms = 600000;
        config.dynamic_snitch_badness_threshold = 0.1;
        config.request_scheduler = org.apache.cassandra.scheduler.NoScheduler.class.getName();
        config.index_interval = 128;
        config.server_encryption_options.internode_encryption = InternodeEncryption.none;
        config.server_encryption_options.keystore_password = "cassandra";
        config.server_encryption_options.truststore_password = "cassandra";
        config.client_encryption_options.enabled = false;
        config.client_encryption_options.keystore_password = "cassandra";
        config.internode_compression = InternodeCompression.all;
        config.inter_dc_tcp_nodelay = true;

    }

    private void updateWithHomePath(File cassandraHome) {
        String absolutePath = cassandraHome.getAbsolutePath();
        config.client_encryption_options.keystore = absolutePath + "/keystore";
        config.data_file_directories = new String[] { absolutePath + "/data" };
        config.commitlog_directory = absolutePath + "/commitlog";
        config.server_encryption_options.keystore = absolutePath + "/keystore";
        config.server_encryption_options.truststore = absolutePath + "/truststore";
        config.client_encryption_options.keystore = absolutePath + "/keystore";
        config.saved_caches_directory = absolutePath + "saved_caches";
    }

    public void load() {
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(configFile);
            config = (Config) getYaml().load(stream);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot write Cassandra configuration file : " + configFile, e);
        } finally
        {
            if (stream != null)
                try {
                    stream.close();
                } catch (IOException e) {
                    throw new IllegalStateException("Cannot write Cassandra configuration file : " + configFile, e);
                }
        }
    }

    public void write() {
        log.info(" Temporary cassandra.yaml file = {}", configFile.getAbsolutePath());
        updateWithHomePath(cassandraHome);
        try {
            configFile.getParentFile().mkdirs();
            if (configFile.exists())
                configFile.delete();

            configFile.createNewFile();
        } catch (IOException e1) {
            throw new IllegalStateException("Cannot create config file", e1);
        }
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(configFile);
            String data = getYaml().dump(config);
            printWriter.println(data);
            printWriter.println();
            printWriter.println("seed_provider:");
            printWriter.println("    - class_name: org.apache.cassandra.locator.SimpleSeedProvider");
            printWriter.println("      parameters:");
            printWriter.println("          - seeds: \"127.0.0.1\"");

        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Cannot write Cassandra configuration to file : " + configFile, e);
        } finally
        {
            if (printWriter != null)
            {
                printWriter.close();
            }
        }
    }

    private Yaml getYaml() {
        org.yaml.snakeyaml.constructor.Constructor constructor = new org.yaml.snakeyaml.constructor.Constructor(
                Config.class);
        TypeDescription seedDesc = new TypeDescription(SeedProviderDef.class);
        seedDesc.putMapPropertyType("parameters", String.class, String.class);
        constructor.addTypeDescription(seedDesc);
        Yaml yaml = new Yaml(new Loader(constructor));
        return yaml;
    }

    public Config underlyingConfig() {
        return config;
    }

    public File getCassandraHome() {
        return cassandraHome;
    }

    public File getConfigFile() {
        return configFile;
    }

    public int getCqlPort() {
        return config.native_transport_port;
    }

    public int getRPCPort() {
        return config.rpc_port;
    }

    public String getCqlHost() {
        return config.listen_address;
    }

    public CassandraConfig cqlPort(int port) {
        config.native_transport_port = port;
        return this;
    }

    public CassandraConfig thriftPort(int port) {
        config.rpc_port = port;
        return this;
    }

    public CassandraConfig randomPorts() {
        storageRandomPort();
        storageSslRandomPort();
        cqlRandomPort();
        thriftRandomPort();
        return this;
    }

    public CassandraConfig storageRandomPort() {
        config.storage_port = PortFinder.findAvailableBetween(7001, 7500);
        return this;
    }

    public CassandraConfig storageSslRandomPort() {
        config.ssl_storage_port = PortFinder.findAvailableBetween(7501, 7999);
        return this;
    }

    public CassandraConfig cqlRandomPort() {
        config.native_transport_port = PortFinder.findAvailableBetween(9001, 9499);
        return this;
    }

    public CassandraConfig thriftRandomPort() {
        config.rpc_port = PortFinder.findAvailableBetween(9501, 9999);
        return this;
    }

}
