package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.embedded.CassandraConfig.cqlRandomPort;
import static info.archinnov.achilles.embedded.CassandraConfig.storageRandomPort;
import static info.archinnov.achilles.embedded.CassandraConfig.storageSslRandomPort;
import static info.archinnov.achilles.embedded.CassandraConfig.thriftRandomPort;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_CQL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_STORAGE_SSL_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_THRIFT_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_CONFIG_FILE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CLEAN_CASSANDRA_DATA_FILES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.COMMIT_LOG_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CONFIG_YAML_FILE;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DATA_FILE_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_ACHILLES_TEST_FOLDERS;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.SAVED_CACHES_FOLDER;
import static info.archinnov.achilles.embedded.CassandraEmbeddedServerStarter.CASSANDRA_EMBEDDED;

import java.io.File;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import info.archinnov.achilles.validation.Validator;

public class AchillesEmbeddedServer {

    public static final String CASSANDRA_HOST = "cassandraHost";

    public static final Logger log = LoggerFactory.getLogger(AchillesEmbeddedServer.class);

    private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("[a-zA-Z][_a-zA-Z0-9]{0,31}");

    private static int cqlPort;

    private static int thriftPort;

    public static int getThriftPort() {
        return thriftPort;
    }

    public static int getCqlPort() {
        return cqlPort;
    }

    protected void startServer(Map<String, Object> parameters) {
        String cassandraHost = System.getProperty(CASSANDRA_HOST);
        if (StringUtils.isBlank(cassandraHost)) {

            validateDataFolders(parameters);
            cleanCassandraDataFiles(parameters);
            cleanCassandraConfigFile(parameters);
            randomizePortsIfNeeded(parameters);

            CassandraConfig cassandraConfig = new CassandraConfig(parameters);

            // Start embedded server
            CASSANDRA_EMBEDDED.start(cassandraConfig);
        }
    }

    protected String extractAndValidateKeyspaceName(Map<String, Object> parameters) {
        String keyspaceName = (String) parameters.get(KEYSPACE_NAME);
        Validator.validateNotBlank(keyspaceName, "The provided keyspace name should not be blank");
        Validator.validateTrue(KEYSPACE_NAME_PATTERN.matcher(keyspaceName)
                                                    .matches(), "The provided keyspace name '%s' should match the " +
                                       "following pattern : '%s'", keyspaceName,KEYSPACE_NAME_PATTERN.pattern());

        return keyspaceName;
    }

    private void validateDataFolders(Map<String, Object> parameters) {
        final String dataFolder = (String) parameters.get(DATA_FILE_FOLDER);
        final String commitLogFolder = (String) parameters.get(COMMIT_LOG_FOLDER);
        final String savedCachesFolder = (String) parameters.get(SAVED_CACHES_FOLDER);

        log.debug(" Embedded Cassandra data directory = {}", dataFolder);
        log.debug(" Embedded Cassandra commitlog directory = {}", commitLogFolder);
        log.debug(" Embedded Cassandra saved caches directory = {}", savedCachesFolder);

        validateFolder(dataFolder);
        validateFolder(commitLogFolder);
        validateFolder(savedCachesFolder);

    }

    private void validateFolder(String folderPath) {
        String currentUser = System.getProperty("user.name");
        final File folder = new File(folderPath);
        if (!DEFAULT_ACHILLES_TEST_FOLDERS.contains(folderPath)) {
            Validator.validateTrue(folder.exists(), "Folder '%s' does not exist", folder.getAbsolutePath());
            Validator.validateTrue(folder.isDirectory(), "Folder '%s' is not a directory", folder.getAbsolutePath());
            Validator
                    .validateTrue(folder.canRead(), "No read credential. Please grant read permission for the current" +
                            " " +
                            "user '%s' on folder '%s'", currentUser, folder
                                          .getAbsolutePath());
            Validator
                    .validateTrue(folder.canWrite(), "No write credential. Please grant write permission for the " +
                            "current " +
                            "user '%s' on folder '%s'", currentUser, folder
                                          .getAbsolutePath());
        }
    }

    private void cleanCassandraDataFiles(Map<String, Object> parameters) {
        if ((Boolean) parameters.get(CLEAN_CASSANDRA_DATA_FILES)) {
            final ImmutableSet<String> dataFolders = ImmutableSet.<String>builder()
                                                                 .add((String) parameters.get(DATA_FILE_FOLDER))
                                                                 .add((String) parameters.get(COMMIT_LOG_FOLDER))
                                                                 .add((String) parameters.get(SAVED_CACHES_FOLDER))
                                                                 .build();
            for (String dataFolder : dataFolders) {
                File dataFolderFile = new File(dataFolder);
                if (dataFolderFile.exists() && dataFolderFile.isDirectory()) {
                    log.info("Cleaning up embedded Cassandra data directory '{}'", dataFolderFile.getAbsolutePath());
                    FileUtils.deleteQuietly(dataFolderFile);
                }
            }
        }
    }

    private void cleanCassandraConfigFile(Map<String, Object> parameters) {
        if ((Boolean) parameters.get(CLEAN_CASSANDRA_CONFIG_FILE)) {
            String configYamlFilePath = (String) parameters.get(CONFIG_YAML_FILE);
            final File configYamlFile = new File(configYamlFilePath);
            if (configYamlFile.exists()) {
                String currentUser = System.getProperty("user.name");
                Validator.validateTrue(configYamlFile
                                               .canWrite(), "No write credential. Please grant write permission for " +
                                               "the current user '%s' on file '%s'", currentUser, configYamlFile
                                               .getAbsolutePath());
                configYamlFile.delete();
            }
        }
    }

    private void randomizePortsIfNeeded(Map<String, Object> parameters) {
        final Integer thriftPort = extractAndValidatePort(Optional
                                                                  .fromNullable(parameters.get(CASSANDRA_THRIFT_PORT))
                                                                  .or(thriftRandomPort()), CASSANDRA_THRIFT_PORT);
        final Integer cqlPort = extractAndValidatePort(Optional
                                                               .fromNullable(parameters.get(CASSANDRA_CQL_PORT))
                                                               .or(cqlRandomPort()), CASSANDRA_CQL_PORT);
        final Integer storagePort = extractAndValidatePort(Optional
                                                                   .fromNullable(parameters.get(CASSANDRA_STORAGE_PORT))
                                                                   .or(storageRandomPort()), CASSANDRA_STORAGE_PORT);
        final Integer storageSSLPort = extractAndValidatePort(Optional
                                                                      .fromNullable(parameters
                                                                                            .get
                                                                                                    (CASSANDRA_STORAGE_SSL_PORT))
                                                                      .or(storageSslRandomPort()),
                                                              CASSANDRA_STORAGE_SSL_PORT);

        parameters.put(CASSANDRA_THRIFT_PORT, thriftPort);
        parameters.put(CASSANDRA_CQL_PORT, cqlPort);
        parameters.put(CASSANDRA_STORAGE_PORT, storagePort);
        parameters.put(CASSANDRA_STORAGE_SSL_PORT, storageSSLPort);

        AchillesEmbeddedServer.cqlPort = cqlPort;
        AchillesEmbeddedServer.thriftPort = thriftPort;
    }

    private Integer extractAndValidatePort(Object port, String portLabel) {
        Validator.validateTrue(port instanceof Integer, "The provided '%s' port should be an integer", portLabel);
        Validator.validateTrue((Integer) port > 0, "The provided '%s' port should positive", portLabel);
        return (Integer) port;

    }
}
