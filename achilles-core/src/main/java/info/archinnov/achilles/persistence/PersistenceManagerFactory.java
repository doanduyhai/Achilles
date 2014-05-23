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
package info.archinnov.achilles.persistence;

import static info.archinnov.achilles.configuration.ConfigurationParameters.BEAN_VALIDATION_ENABLE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.BEAN_VALIDATION_VALIDATOR;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CLUSTER_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.COMPRESSION_TYPE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONNECTION_CONTACT_POINTS_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONNECTION_CQL_PORT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_DEFAULT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_MAP_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_MAP_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.DISABLE_JMX;
import static info.archinnov.achilles.configuration.ConfigurationParameters.DISABLE_METRICS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITIES_LIST_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.EVENT_INTERCEPTORS_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.LOAD_BALANCING_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.NATIVE_SESSION_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_FACTORY_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PASSWORD;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PREPARED_STATEMENTS_CACHE_SIZE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.RECONNECTION_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.RETRY_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.SSL_ENABLED;
import static info.archinnov.achilles.configuration.ConfigurationParameters.SSL_OPTIONS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.USERNAME;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.SchemaContext;
import info.archinnov.achilles.internal.metadata.discovery.AchillesBootstrapper;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.ParsingResult;
import info.archinnov.achilles.internal.proxy.ProxyClassFactory;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.TypedMap;

public class PersistenceManagerFactory {
    private static final Logger log = LoggerFactory.getLogger(PersistenceManagerFactory.class);

    Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<>();

    ConfigurationContext configContext;

    DaoContext daoContext;

    PersistenceContextFactory contextFactory;

    TypedMap configurationMap;

    private ArgumentExtractor argumentExtractor = new ArgumentExtractor();

    private AchillesBootstrapper bootstrapper = new AchillesBootstrapper();

    private ProxyClassFactory proxyClassFactory = new ProxyClassFactory();

    /**
     * Create a new PersistenceManagerFactory with a configuration map
     *
     * @param configurationMap Check documentation for more details on configuration
     *                         parameters
     */
    PersistenceManagerFactory(Map<String, Object> configurationMap) {
        Validator.validateNotNull(configurationMap,
                "Configuration map for PersistenceManagerFactory should not be null");
        Validator.validateNotEmpty(configurationMap,
                "Configuration map for PersistenceManagerFactory should not be empty");
        this.configurationMap = TypedMap.fromMap(configurationMap);
    }

    PersistenceManagerFactory bootstrap() {
        final String keyspaceName = configurationMap.getTyped(KEYSPACE_NAME_PARAM);

        log.info("Bootstrapping Achilles PersistenceManagerFactory for keyspace {}", keyspaceName);

        configContext = argumentExtractor.initConfigContext(configurationMap);
        Cluster cluster = argumentExtractor.initCluster(configurationMap);
        Session session = argumentExtractor.initSession(cluster, configurationMap);
        List<Interceptor<?>> interceptors = argumentExtractor.initInterceptors(configurationMap);
        List<Class<?>> candidateClasses = argumentExtractor.initEntities(configurationMap);

        ParsingResult parsingResult = parseEntities(candidateClasses);
        this.entityMetaMap = parsingResult.getMetaMap();

        bootstrapper.addInterceptorsToEntityMetas(interceptors, parsingResult.getMetaMap());

        SchemaContext schemaContext = new SchemaContext(configContext.isForceColumnFamilyCreation(), session,
                keyspaceName, cluster, parsingResult);
        bootstrapper.validateOrCreateTables(schemaContext);

        daoContext = bootstrapper.buildDaoContext(session, parsingResult, configContext);
        contextFactory = new PersistenceContextFactory(daoContext, configContext, parsingResult.getMetaMap());
        registerShutdownHook(cluster);

        warmUpProxies();

        return this;
    }


    private void warmUpProxies() {
        if (argumentExtractor.initProxyWarmUp(configurationMap)) {
            long start = System.nanoTime();
            for (Class<?> clazz : entityMetaMap.keySet()) {
                proxyClassFactory.createProxyClass(clazz);
            }
            long end = System.nanoTime();
            long duration = (end - start) / 1000000;
            log.info("Entity proxies warm up took {} millisecs for {} entities", duration, entityMetaMap.size());
        }
    }

    private ParsingResult parseEntities(List<Class<?>> candidateClasses) {
        return bootstrapper.buildMetaDatas(configContext, candidateClasses);
    }

    /**
     * Create a new PersistenceManager. This instance of PersistenceManager is
     * <strong>thread-safe</strong>
     *
     * @return PersistenceManager
     */
    public PersistenceManager createPersistenceManager() {
        log.debug("Spawn new PersistenceManager");
        return new PersistenceManager(entityMetaMap, contextFactory, daoContext, configContext);
    }

    /**
     * Create a new state-full PersistenceManager for batch handling <br/>
     * <br/>
     * <p/>
     * <strong>WARNING : This PersistenceManager is state-full and not
     * thread-safe. In case of exception, you MUST not re-use it but create
     * another one</strong>
     *
     * @return a new state-full PersistenceManager
     */
    public BatchingPersistenceManager createBatchingPersistenceManager() {
        log.debug("Spawn new BatchingPersistenceManager");
        return new BatchingPersistenceManager(entityMetaMap, contextFactory, daoContext, configContext);
    }

    /**
     * Serialize the entity in JSON using a registered Object Mapper or default Achilles Object Mapper
     * @param entity
     * @return serialized entity in JSON
     * @throws IOException
     */
    public String jsonSerialize(Object entity) throws IOException {
        Validator.validateNotNull(entity, "Cannot serialize to JSON null entity");
        final ObjectMapper objectMapper = configContext.getMapperFor(entity.getClass());
        return objectMapper.writeValueAsString(entity);
    }

    /**
     * Deserialize the given JSON into entity using a registered Object Mapper or default Achilles Object Mapper
     * @param type
     * @param serialized
     * @param <T>
     * @return deserialized entity from JSON
     * @throws IOException
     */
    public <T> T deserializeJson(Class<T> type, String serialized) throws IOException {
        Validator.validateNotNull(type, "Cannot deserialize from JSON if target type is null");
        final ObjectMapper objectMapper = configContext.getMapperFor(type);
        return objectMapper.readValue(serialized, type);
    }

    private void registerShutdownHook(final Cluster cluster) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.close();
            }
        });
    }

    public static class PersistenceManagerFactoryBuilder {

        private TypedMap configMap = new TypedMap();

        private PersistenceManagerFactoryBuilder() {
        }

        /**
         * Create a new PersistenceManagerFactory with the given configuration
         * map
         *
         * @param configurationMap configuration map
         */
        public static PersistenceManagerFactory build(Map<String, Object> configurationMap) {
            return new PersistenceManagerFactory(configurationMap).bootstrap();
        }

        /**
         * Create a new builder to configure each parameter
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public static PersistenceManagerFactoryBuilder builder() {
            return new PersistenceManagerFactoryBuilder();
        }

        /**
         * Define entity packages to scan for '@Entity' classes The packages
         * should be comma-separated
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withEntityPackages(String entityPackages) {
            configMap.put(ENTITY_PACKAGES_PARAM, entityPackages);
            return this;
        }

        /**
         * Define entities
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withEntities(List<Class<?>> entities) {
            configMap.put(ENTITIES_LIST_PARAM, entities);
            return this;
        }

        /**
         * Define a pre-configured Jackson Object Mapper for serialization of
         * non-primitive types
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withObjectMapper(ObjectMapper objectMapper) {
            configMap.put(OBJECT_MAPPER_PARAM, objectMapper);
            return this;
        }

        /**
         * Define a pre-configured map of Jackson Object Mapper for
         * serialization of non-primitive types
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withObjectMapperFactory(ObjectMapperFactory objectMapperFactory) {
            configMap.put(OBJECT_MAPPER_FACTORY_PARAM, objectMapperFactory);
            return this;
        }

        /**
         * Define the default Consistency level to be used for all READ
         * operations
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withDefaultReadConsistency(String defaultReadConsistency) {
            configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM, defaultReadConsistency);
            return this;
        }

        /**
         * Define the default Consistency level to be used for all WRITE
         * operations
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withDefaultWriteConsistency(String defaultWriteConsistency) {
            configMap.put(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM, defaultWriteConsistency);
            return this;
        }

        /**
         * Define the default Consistency level map to be used for all READ
         * operations The map keys represent table names and values represent
         * the corresponding consistency level
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withDefaultReadConsistencyMap(Map<String, String> readConsistencyMap) {
            configMap.put(CONSISTENCY_LEVEL_READ_MAP_PARAM, readConsistencyMap);
            return this;
        }

        /**
         * Define the default Consistency level map to be used for all WRITE
         * operations The map keys represent table names and values represent
         * the corresponding consistency level
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withDefaultWriteConsistencyMap(Map<String,
                String> writeConsistencyMap) {
            configMap.put(CONSISTENCY_LEVEL_WRITE_MAP_PARAM, writeConsistencyMap);
            return this;
        }

        /**
         * Whether Achilles should force table creation if they do not already
         * exist in the keyspace This flag is useful for dev only. <strong>It
         * should be disabled in production</strong>
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder forceTableCreation(boolean forceTableCreation) {
            configMap.put(FORCE_TABLE_CREATION_PARAM, forceTableCreation);
            return this;
        }

        /**
         * Define the pre-configured com.datastax.driver.core.Cluster object to
         * be used instead of creating a new one
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withCluster(Cluster cluster) {
            configMap.put(CLUSTER_PARAM, cluster);
            return this;
        }

        /**
         * Define the pre-configured com.datastax.driver.core.Session object to
         * be used instead of creating a new one
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withNativeSession(Session nativeSession) {
            configMap.put(NATIVE_SESSION_PARAM, nativeSession);
            return this;
        }

        /**
         * Define the contact points to connect to. The contact point list is
         * comma-separated
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withConnectionContactPoints(String contactPoints) {
            configMap.put(CONNECTION_CONTACT_POINTS_PARAM, contactPoints);
            return this;
        }

        /**
         * Define the CQL port to connect to. Default value is 9042
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withCQLPort(Integer cqlPort) {
            configMap.put(CONNECTION_CQL_PORT_PARAM, cqlPort);
            return this;
        }

        /**
         * Define the keyspace name to be used by Achilles. Note: you should
         * build as many PersistenceManagerFactory as different keyspaces to be
         * used
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withKeyspaceName(String keyspaceName) {
            configMap.put(KEYSPACE_NAME_PARAM, keyspaceName);
            return this;
        }

        /**
         * Define the com.datastax.driver.core.ProtocolOptions.Compression type
         * to be used
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withCompressionType(ProtocolOptions.Compression compressionType) {
            configMap.put(COMPRESSION_TYPE, compressionType);
            return this;
        }

        /**
         * Define the com.datastax.driver.core.policies.RetryPolicy to be used
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withRetryPolicy(RetryPolicy retryPolicy) {
            configMap.put(RETRY_POLICY, retryPolicy);
            return this;
        }

        /**
         * Define the com.datastax.driver.core.policies.LoadBalancingPolicy to
         * be used
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
            configMap.put(LOAD_BALANCING_POLICY, loadBalancingPolicy);
            return this;
        }

        /**
         * Define the com.datastax.driver.core.policies.ReconnectionPolicy to be
         * used
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
            configMap.put(RECONNECTION_POLICY, reconnectionPolicy);
            return this;
        }

        /**
         * Define the username to connect to the cluster
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withUsername(String username) {
            configMap.put(USERNAME, username);
            return this;
        }

        /**
         * Define the password to connect to the cluster
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withPassword(String password) {
            configMap.put(PASSWORD, password);
            return this;
        }

        /**
         * Whether JMX should be disabled
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder disableJMX(Boolean disableJmx) {
            configMap.put(DISABLE_JMX, disableJmx);
            return this;
        }

        /**
         * Whether metrics should be disabled
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder disableMetrics(Boolean disableMetrics) {
            configMap.put(DISABLE_METRICS, disableMetrics);
            return this;
        }

        /**
         * Whether to enable SSL connection
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder enableSSL(Boolean enableSSL) {
            configMap.put(SSL_ENABLED, enableSSL);
            return this;
        }

        /**
         * Define the com.datastax.driver.core.SSLOptions to be used
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withSSLOptions(SSLOptions sslOptions) {
            configMap.put(SSL_OPTIONS, sslOptions);
            return this;
        }

        /**
         * Provide a list of event interceptors
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withEventInterceptors(List<Interceptor<?>> interceptors) {
            configMap.put(EVENT_INTERCEPTORS_PARAM, interceptors);
            return this;
        }

        /**
         * Activate Bean Validation (JSR303)
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder enableBeanValidation(boolean enableBeanValidation) {
            configMap.put(BEAN_VALIDATION_ENABLE, enableBeanValidation);
            return this;
        }

        /**
         * Provide custom validator for Bean Validation (JSR303)
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withBeanValidator(javax.validation.Validator validator) {
            if (validator != null) {
                configMap.put(BEAN_VALIDATION_VALIDATOR, validator);
            }
            return this;
        }

        /**
         * Specify maximum size for the internal prepared statements LRU cache.
         * If the cache is full, oldest prepared statements will be dropped, leading to unexpected behavior.
         * <br/><br/>
         * Default value is <strong>5000</strong>, which is a pretty safe limit.
         * <br/><br/>
         * For information, only selects on counter fields and updates are put into the cache because they cannot be
         * prepared before hand since the updated properties are not known in advance.
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withMaxPreparedStatementCacheSize(int maxPreparedStatementCacheSize) {
            configMap.put(PREPARED_STATEMENTS_CACHE_SIZE, maxPreparedStatementCacheSize);
            return this;
        }

        /**
         * Build a new PersistenceManagerFactory
         *
         * @return PersistenceManagerFactory
         */
        public PersistenceManagerFactory build() {
            return new PersistenceManagerFactory(configMap).bootstrap();
        }
    }
}
