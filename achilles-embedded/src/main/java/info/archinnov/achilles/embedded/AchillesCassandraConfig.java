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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ConfigurationLoader;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;

public class AchillesCassandraConfig implements ConfigurationLoader {

    static final String ACHILLES_EMBEDDED_CASSANDRA_THRIFT_PORT = "ACHILLES_EMBEDDED_CASSANDRA_THRIFT_PORT";
    static final String ACHILLES_EMBEDDED_CASSANDRA_CQL_PORT = "ACHILLES_EMBEDDED_CASSANDRA_CQL_PORT";
    static final String ACHILLES_EMBEDDED_CASSANDRA_STORAGE_PORT = "ACHILLES_EMBEDDED_CASSANDRA_STORAGE_PORT";
    static final String ACHILLES_EMBEDDED_CASSANDRA_STORAGE_SSL_PORT = "ACHILLES_EMBEDDED_CASSANDRA_STORAGE_SSL_PORT";
    static final String ACHILLES_EMBEDDED_CASSANDRA_LISTEN_ADDRESS = "ACHILLES_EMBEDDED_CASSANDRA_LISTEN_ADDRESS";
    static final String ACHILLES_EMBEDDED_CASSANDRA_BROADCAST_ADDRESS = "ACHILLES_EMBEDDED_CASSANDRA_BROADCAST_ADDRESS";
    static final String ACHILLES_EMBEDDED_CASSANDRA_RPC_ADDRESS = "ACHILLES_EMBEDDED_CASSANDRA_RPC_ADDRESS";
    static final String ACHILLES_EMBEDDED_CASSANDRA_BROADCAST_RPC_ADDRESS = "ACHILLES_EMBEDDED_CASSANDRA_BROADCAST_RPC_ADDRESS";


    static final String ACHILLES_EMBEDDED_CASSANDRA_DATA_FOLDER = "ACHILLES_EMBEDDED_CASSANDRA_DATA_FOLDER";
    static final String ACHILLES_EMBEDDED_CASSANDRA_COMMITLOG_FOLDER = "ACHILLES_EMBEDDED_CASSANDRA_COMMITLOG_FOLDER";
    static final String ACHILLES_EMBEDDED_CASSANDRA_SAVED_CACHES_FOLDER = "ACHILLES_EMBEDDED_CASSANDRA_SAVED_CACHES_FOLDER";
    static final String ACHILLES_EMBEDDED_CASSANDRA_HINTS_FOLDER = "ACHILLES_EMBEDDED_CASSANDRA_HINTS_FOLDER";
    static final String ACHILLES_EMBEDDED_CASSANDRA_CDC_RAW_FOLDER = "ACHILLES_EMBEDDED_CASSANDRA_CDC_RAW_FOLDER";

    @Override
    public Config loadConfig() throws ConfigurationException {
        final Config config = new Config();

        final int numTokens = Integer.parseInt(System.getProperty("cassandra-num-tokens", "256"));
        config.num_tokens = numTokens;

        config.listen_address = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_LISTEN_ADDRESS);
        config.rpc_address = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_RPC_ADDRESS);
        final String broadcastAddress = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_BROADCAST_ADDRESS);

        if(isNotBlank(broadcastAddress))
            config.broadcast_address = broadcastAddress;

        final String broadcastRPCAddress = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_BROADCAST_RPC_ADDRESS);
        if(isNotBlank(broadcastRPCAddress))
            config.broadcast_rpc_address = broadcastRPCAddress;

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
        config.start_native_transport = true;
        config.start_rpc = false;
        config.rpc_keepalive = true;
        config.rpc_server_type = "sync";
        config.thrift_framed_transport_size_in_mb = 15;
        config.incremental_backups = false;
        config.snapshot_before_compaction = false;
        config.auto_snapshot = false;
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
        config.enable_user_defined_functions = true;
        config.enable_user_defined_functions_threads = true;
        config.enable_scripted_user_defined_functions = false;

        // Tuning for perf
        config.memtable_heap_space_in_mb = 64;
        config.commitlog_total_space_in_mb = 32;

        config.disk_failure_policy = Config.DiskFailurePolicy.stop_paranoid;

        final Map<String, String> seedsMap = new HashMap<>();
        seedsMap.put("seeds", config.listen_address);
        config.seed_provider = new ParameterizedClass("org.apache.cassandra.locator.SimpleSeedProvider", seedsMap);

        config.data_file_directories = new String[]{System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_DATA_FOLDER)};
        config.commitlog_directory = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_COMMITLOG_FOLDER);
        config.saved_caches_directory = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_SAVED_CACHES_FOLDER);
        config.hints_directory = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_HINTS_FOLDER);
        config.cdc_raw_directory = System.getProperty(ACHILLES_EMBEDDED_CASSANDRA_CDC_RAW_FOLDER);
        return config;
    }
}
