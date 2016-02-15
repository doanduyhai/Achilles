package info.archinnov.achilles.embedded;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ConfigurationLoader;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class AchillesCassandraConfig implements ConfigurationLoader {

    static final String ACHILLES_EMBEDDED_CASSANDRA_THRIFT_PORT = "ACHILLES_EMBEDDED_CASSANDRA_THRIFT_PORT";
    static final String ACHILLES_EMBEDDED_CASSANDRA_CQL_PORT = "ACHILLES_EMBEDDED_CASSANDRA_CQL_PORT";
    static final String ACHILLES_EMBEDDED_CASSANDRA_STORAGE_PORT = "ACHILLES_EMBEDDED_CASSANDRA_STORAGE_PORT";
    static final String ACHILLES_EMBEDDED_CASSANDRA_STORAGE_SSL_PORT = "ACHILLES_EMBEDDED_CASSANDRA_STORAGE_SSL_PORT";
    static final String ACHILLES_EMBEDDED_CASSANDRA_JMX_PORT = "ACHILLES_EMBEDDED_CASSANDRA_JMX_PORT";

    static final String ACHILLES_EMBEDDED_CASSANDRA_DATA_FOLDER = "ACHILLES_EMBEDDED_CASSANDRA_DATA_FOLDER";
    static final String ACHILLES_EMBEDDED_CASSANDRA_COMMITLOG_FOLDER = "ACHILLES_EMBEDDED_CASSANDRA_COMMITLOG_FOLDER";
    static final String ACHILLES_EMBEDDED_CASSANDRA_SAVED_CACHES_FOLDER = "ACHILLES_EMBEDDED_CASSANDRA_SAVED_CACHES_FOLDER";
    static final String ACHILLES_EMBEDDED_CASSANDRA_HINTS_FOLDER = "ACHILLES_EMBEDDED_CASSANDRA_HINTS_FOLDER";

    @Override
    public Config loadConfig() throws ConfigurationException {
        final Config config = new Config();

        config.rpc_port = Integer.parseInt(System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_THRIFT_PORT));
        config.native_transport_port = Integer.parseInt(System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_CQL_PORT));
        config.storage_port = Integer.parseInt(System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_STORAGE_PORT));
        config.ssl_storage_port = Integer.parseInt(System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_STORAGE_SSL_PORT));

        config.hinted_handoff_enabled = false;
        config.max_hint_window_in_ms = 10800000; // 3 hours
        config.hinted_handoff_throttle_in_kb = 1024;
        config.max_hints_delivery_threads = 2;
        config.authenticator = org.apache.cassandra.auth.AllowAllAuthenticator.class.getName();
        config.authorizer = org.apache.cassandra.auth.AllowAllAuthorizer.class.getName();
        config.permissions_validity_in_ms = 2000;
        config.partitioner = org.apache.cassandra.dht.Murmur3Partitioner.class.getName();
        config.disk_failure_policy = Config.DiskFailurePolicy.stop;
        config.key_cache_save_period = 14400;
        config.row_cache_size_in_mb = 0;
        config.row_cache_save_period = 0;
        config.commitlog_sync = Config.CommitLogSync.periodic;
        config.commitlog_sync_period_in_ms = 10000;
        config.commitlog_segment_size_in_mb = 32;
        config.concurrent_reads = Integer.parseInt(System.getProperty("cassandra.embedded.concurrent.reads"));
        config.concurrent_writes = Integer.parseInt(System.getProperty("cassandra.embedded.concurrent.writes"));
        config.memtable_allocation_type = Config.MemtableAllocationType.heap_buffers;
        config.trickle_fsync = false;
        config.trickle_fsync_interval_in_kb = 10240;
        config.listen_address = "localhost";
        config.start_native_transport = true;
        config.start_rpc = true;
        config.rpc_address = "localhost";
        config.rpc_keepalive = true;
        config.rpc_server_type = "sync";
        config.thrift_framed_transport_size_in_mb = 15;
        config.incremental_backups = false;
        config.snapshot_before_compaction = false;
        config.auto_snapshot = true;
        config.column_index_size_in_kb = 64;
        config.compaction_throughput_mb_per_sec = 16;
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
        config.server_encryption_options.internode_encryption = EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none;
        config.server_encryption_options.keystore_password = "cassandra";
        config.server_encryption_options.truststore_password = "cassandra";
        config.client_encryption_options.enabled = false;
        config.client_encryption_options.keystore_password = "cassandra";
        config.internode_compression = Config.InternodeCompression.all;
        config.inter_dc_tcp_nodelay = true;
        config.broadcast_address = "localhost";
        config.broadcast_rpc_address = "localhost";
        config.enable_user_defined_functions = true;
        config.enable_user_defined_functions_threads = true;
        config.enable_scripted_user_defined_functions = false;

        // Tuning for perf
        config.memtable_heap_space_in_mb = 64;
        config.commitlog_total_space_in_mb = 32;

        config.disk_failure_policy = Config.DiskFailurePolicy.stop_paranoid;

        final Map<String, String> seedsMap = new HashMap<>();
        seedsMap.put("seeds", "localhost");
        config.seed_provider = new ParameterizedClass("org.apache.cassandra.locator.SimpleSeedProvider", seedsMap);

        config.data_file_directories = new String[]{System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_DATA_FOLDER)};
        config.commitlog_directory = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_COMMITLOG_FOLDER);
        config.saved_caches_directory = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_SAVED_CACHES_FOLDER);
        config.hints_directory = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_HINTS_FOLDER);
        return config;
    }
}
