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

/**
 *
 * Modified version of original class from HouseScream
 *
 * https://github.com/housecream/server/blob/develop/server/ws/src/main/java/org/housecream/server/application/CassandraConfig.java
 *
 */

package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_CQL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_CONCURRENT_READS;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_CONCURRENT_WRITES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_SSL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_THRIFT_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.COMMIT_LOG_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CONFIG_YAML_FILE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DATA_FILE_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.SAVED_CACHES_FOLDER;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.Config.InternodeCompression;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.config.SeedProviderDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import info.archinnov.achilles.type.TypedMap;

public class CassandraConfig {

    private final Logger log = LoggerFactory.getLogger(CassandraConfig.class);
    private final File configFile;
    private Map<String, Object> parameters;
    private Config config = new Config();

    public CassandraConfig(TypedMap parameters) {
        this.parameters = parameters;
        this.configFile = new File(parameters.<String>getTyped(CONFIG_YAML_FILE));

        config.rpc_port = parameters.getTyped(CASSANDRA_THRIFT_PORT);
        config.native_transport_port = parameters.getTyped(CASSANDRA_CQL_PORT);
        config.storage_port = parameters.getTyped(CASSANDRA_STORAGE_PORT);
        config.ssl_storage_port = parameters.getTyped(CASSANDRA_STORAGE_SSL_PORT);

        config.hinted_handoff_enabled = "false";
        config.max_hint_window_in_ms = 10800000; // 3 hours
        config.hinted_handoff_throttle_in_kb = 1024;
        config.max_hints_delivery_threads = 2;
        config.authenticator = org.apache.cassandra.auth.AllowAllAuthenticator.class.getName();
        config.authorizer = org.apache.cassandra.auth.AllowAllAuthorizer.class.getName();
        config.permissions_validity_in_ms = 2000;
        config.partitioner = org.apache.cassandra.dht.Murmur3Partitioner.class.getName();
        config.disk_failure_policy = DiskFailurePolicy.stop;
        config.key_cache_save_period = 14400;
        config.row_cache_size_in_mb = 0;
        config.row_cache_save_period = 0;
        config.commitlog_sync = CommitLogSync.periodic;
        config.commitlog_sync_period_in_ms = 10000;
        config.commitlog_segment_size_in_mb = 32;
        config.concurrent_reads = parameters.getTypedOr(CASSANDRA_CONCURRENT_READS, 32);
        config.concurrent_writes = parameters.getTypedOr(CASSANDRA_CONCURRENT_WRITES, 32);
        config.memtable_flush_queue_size = 4;
        config.trickle_fsync = false;
        config.trickle_fsync_interval_in_kb = 10240;
        config.listen_address = "127.0.0.1";
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
        config.server_encryption_options.internode_encryption = InternodeEncryption.none;
        config.server_encryption_options.keystore_password = "cassandra";
        config.server_encryption_options.truststore_password = "cassandra";
        config.client_encryption_options.enabled = false;
        config.client_encryption_options.keystore_password = "cassandra";
        config.internode_compression = InternodeCompression.all;
        config.inter_dc_tcp_nodelay = true;

        // Tuning for perf
        config.memtable_total_space_in_mb = 64;
        config.commitlog_total_space_in_mb = 32;
    }

    private void updateWithHomePath() {
        config.data_file_directories = new String[] { (String) parameters.get(DATA_FILE_FOLDER) };
        config.commitlog_directory = (String) parameters.get(COMMIT_LOG_FOLDER);
        config.saved_caches_directory = (String) parameters.get(SAVED_CACHES_FOLDER);

    }

    public void write() {
        log.info(" Temporary cassandra.yaml file = {}", configFile.getAbsolutePath());
        updateWithHomePath();
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
        } finally {
            if (printWriter != null) {
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
        Yaml yaml = new Yaml(constructor);
        return yaml;
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

    public int getStoragePort() {
        return config.storage_port;
    }

    public int getStorageSSLPort() {
        return config.ssl_storage_port;
    }

    public static int storageRandomPort() {
        return PortFinder.findAvailableBetween(7001, 7500);
    }

    public static int storageSslRandomPort() {
        return PortFinder.findAvailableBetween(7501, 7999);
    }

    public static int cqlRandomPort() {
        return PortFinder.findAvailableBetween(9043, 9499);
    }

    public static int thriftRandomPort() {
        return PortFinder.findAvailableBetween(9501, 9999);
    }

}
