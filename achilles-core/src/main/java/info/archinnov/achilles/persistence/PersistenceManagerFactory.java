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
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_DEFAULT;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_MAP;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_DEFAULT;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_MAP;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENABLE_SCHEMA_UPDATE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENABLE_SCHEMA_UPDATE_FOR_TABLES;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITIES_LIST;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.configuration.ConfigurationParameters.EVENT_INTERCEPTORS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.GLOBAL_INSERT_STRATEGY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.configuration.ConfigurationParameters.NATIVE_SESSION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.JACKSON_MAPPER;
import static info.archinnov.achilles.configuration.ConfigurationParameters.JACKSON_MAPPER_FACTORY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PREPARED_STATEMENTS_CACHE_SIZE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PROXIES_WARM_UP_DISABLED;
import static info.archinnov.achilles.configuration.ConfigurationParameters.RELAX_INDEX_VALIDATION;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import info.archinnov.achilles.internal.provider.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.SchemaContext;
import info.archinnov.achilles.internal.metadata.discovery.AchillesBootstrapper;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.ParsingResult;
import info.archinnov.achilles.internal.proxy.ProxyClassFactory;
import info.archinnov.achilles.internal.utils.ConfigMap;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.json.JacksonMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;

/**
 * <p>
 * <strong>Stateless</strong> factory to create persistence manager.
 * This class is totally <strong></strong>thread-safe</strong> and can be shared by many threads.
 * You should normally have only one instance of PersistenceManagerFactory across the application
 *
 */
public class PersistenceManagerFactory {
    private static final Logger log = LoggerFactory.getLogger(PersistenceManagerFactory.class);

    Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<>();

    ConfigurationContext configContext;

    DaoContext daoContext;

    PersistenceContextFactory contextFactory;

    ConfigMap configurationMap;

    private ArgumentExtractor argumentExtractor = new ArgumentExtractor();
    private AchillesBootstrapper bootstrapper = new AchillesBootstrapper();
    private ProxyClassFactory proxyClassFactory = ProxyClassFactory.Singleton.INSTANCE.get();
    private Cluster cluster;


    PersistenceManagerFactory(Cluster cluster, Map<ConfigurationParameters, Object> configurationMap) {
        this.cluster = cluster;
        Validator.validateNotNull(configurationMap, "Configuration map for PersistenceManagerFactory should not be null");
        Validator.validateNotEmpty(configurationMap, "Configuration map for PersistenceManagerFactory should not be empty");
        this.configurationMap = ConfigMap.fromMap(configurationMap);
    }

    PersistenceManagerFactory bootstrap() {
        final String keyspaceName = configurationMap.getTyped(KEYSPACE_NAME);

        log.info("Bootstrapping Achilles PersistenceManagerFactory for keyspace {}", keyspaceName);

        configContext = argumentExtractor.initConfigContext(configurationMap);
        Session session = argumentExtractor.initSession(cluster, configurationMap);
        final ClassLoader classLoader = argumentExtractor.initOSGIClassLoader(configurationMap);
        List<Interceptor<?>> interceptors = argumentExtractor.initInterceptors(configurationMap);
        List<Class<?>> candidateClasses = argumentExtractor.initEntities(configurationMap, classLoader);

        ParsingResult parsingResult = parseEntities(candidateClasses);
        this.entityMetaMap = parsingResult.getMetaMap();

        bootstrapper.addInterceptorsToEntityMetas(interceptors, parsingResult.getMetaMap());

        SchemaContext schemaContext = new SchemaContext(configContext, session, keyspaceName, cluster, parsingResult);
        bootstrapper.validateOrCreateTables(schemaContext);

        daoContext = bootstrapper.buildDaoContext(session, parsingResult, configContext);
        contextFactory = new PersistenceContextFactory(daoContext, configContext, parsingResult.getMetaMap());

        warmUpProxies();

        return this;
    }


    private void warmUpProxies() {
        if (argumentExtractor.initProxyWarmUp(configurationMap)) {
            long start = System.nanoTime();
            for (Class<?> clazz : entityMetaMap.keySet()) {
                proxyClassFactory.createProxyClass(clazz, configContext);
            }
            long end = System.nanoTime();
            long duration = (end - start) / 1000000;
            log.info("Entity proxies warm up took {} milli secs for {} entities", duration, entityMetaMap.size());
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
     * Create a new state-full Batch <br/>
     * <br/>
     * <p/>
     * <strong>WARNING : This Batch is state-full and not
     * thread-safe. In case of exception, you MUST not re-use it but create
     * another one</strong>
     *
     * @return a new state-full PersistenceManager
     */
    public Batch createBatch() {
        log.debug("Spawn new BatchingPersistenceManager");
        return new Batch(entityMetaMap, contextFactory, daoContext, configContext, false);
    }


    /**
     * Create a new state-full <strong>ordered</strong> Batch <br/>
     * <br/>
     * <p>
     * This Batch respect insertion order by generating increasing timestamp with micro second resolution.
     * If you use ordered Batch in multiple clients, do not forget to synchronize the clock between those clients
     * to avoid statements interleaving
     * </p>
     * <strong>WARNING : This Batch is state-full and not
     * thread-safe. In case of exception, you MUST not re-use it but create
     * another one</strong>
     *
     * @return a new state-full PersistenceManager
     */
    public Batch createOrderedBatch() {
        log.debug("Spawn new BatchingPersistenceManager");
        return new Batch(entityMetaMap, contextFactory, daoContext, configContext, true);
    }

    /**
     * Serialize the entity in JSON using a registered Object Mapper or default Achilles Object Mapper
     * @param entity
     * @return serialized entity in JSON
     * @throws IOException
     */
    public String serializeToJSON(Object entity) throws IOException {
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
    public <T> T deserializeFromJSON(Class<T> type, String serialized) throws IOException {
        Validator.validateNotNull(type, "Cannot deserialize from JSON if target type is null");
        final ObjectMapper objectMapper = configContext.getMapperFor(type);
        return objectMapper.readValue(serialized, type);
    }


    public static class PersistenceManagerFactoryBuilder {

        private ConfigMap configMap = new ConfigMap();
        private Cluster cluster;

        private PersistenceManagerFactoryBuilder(Cluster cluster) {
            this.cluster = cluster;
            Validator.validateNotNull(cluster, "Cluster object should not be null");

        }

        /**
         * Create a new PersistenceManagerFactory with the given configuration
         * map
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#parameters" target="_blank">Configuration parameters</a>
         *
         * @param cluster pre-configured {@link com.datastax.driver.core.Cluster}
         * @param configurationMap configuration map
         * @return new PersistenceManagerFactory
         *
         */
        public static PersistenceManagerFactory build(Cluster cluster, Map<ConfigurationParameters, Object> configurationMap) {
            return new PersistenceManagerFactory(cluster, configurationMap).bootstrap();
        }

        /**
         * Create a new builder to configure each parameter
         *
         * @param cluster pre-configured {@link com.datastax.driver.core.Cluster}
         * @return PersistenceManagerFactoryBuilder
         */
        public static PersistenceManagerFactoryBuilder builder(Cluster cluster) {
            return new PersistenceManagerFactoryBuilder(cluster);
        }

        /**
         * Define entity packages to scan for '@Entity' classes The packages
         * should be comma-separated
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#entity-parsing" target="_blank">Entity parsing</a>
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withEntityPackages(String entityPackages) {
            configMap.put(ENTITY_PACKAGES, entityPackages);
            return this;
        }

        /**
         * Define list of entities to be managed by Achilles
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#entity-parsing" target="_blank">Entity parsing</a>
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withEntities(List<Class<?>> entities) {
            configMap.put(ENTITIES_LIST, entities);
            return this;
        }

        /**
         * Define a pre-configured Jackson Object Mapper for serialization of
         * non-primitive types
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#json-serialization" target="_blank">JSON serialization</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withJacksonMapper(ObjectMapper objectMapper) {
            configMap.put(JACKSON_MAPPER, objectMapper);
            return this;
        }

        /**
         * Define a pre-configured map of Jackson Object Mapper for
         * serialization of non-primitive types
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#json-serialization" target="_blank">JSON serialization</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withJacksonMapperFactory(JacksonMapperFactory jacksonMapperFactory) {
            configMap.put(JACKSON_MAPPER_FACTORY, jacksonMapperFactory);
            return this;
        }

        /**
         * Define the default Consistency level to be used for all READ
         * operations
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#consistency-level" target="_blank">Consistency configuration</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withDefaultReadConsistency(ConsistencyLevel defaultReadConsistency) {
            configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT, defaultReadConsistency);
            return this;
        }

        /**
         * Define the default Consistency level to be used for all WRITE
         * operations
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#consistency-level" target="_blank">Consistency configuration</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withDefaultWriteConsistency(ConsistencyLevel defaultWriteConsistency) {
            configMap.put(CONSISTENCY_LEVEL_WRITE_DEFAULT, defaultWriteConsistency);
            return this;
        }

        /**
         * Define the default Consistency level map to be used for all READ
         * operations The map keys represent table names and values represent
         * the corresponding consistency level
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#consistency-level" target="_blank">Consistency configuration</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withDefaultReadConsistencyMap(Map<String, ConsistencyLevel> readConsistencyMap) {
            configMap.put(CONSISTENCY_LEVEL_READ_MAP, readConsistencyMap);
            return this;
        }

        /**
         * Define the default Consistency level map to be used for all WRITE
         * operations The map keys represent table names and values represent
         * the corresponding consistency level
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#consistency-level" target="_blank">Consistency configuration</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withDefaultWriteConsistencyMap(Map<String, ConsistencyLevel> writeConsistencyMap) {
            configMap.put(CONSISTENCY_LEVEL_WRITE_MAP, writeConsistencyMap);
            return this;
        }

        /**
         * Whether Achilles should force table creation if they do not already
         * exist in the keyspace This flag is useful for dev only. <strong>It
         * should be disabled in production</strong>
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#ddl" target="_blank">Table generation</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder forceTableCreation(boolean forceTableCreation) {
            configMap.put(FORCE_TABLE_CREATION, forceTableCreation);
            return this;
        }

        /**
         * Whether Achilles should force update table if entities have new fields and table not.
         * This flag is useful for dev only. <strong>It is strongly advised to disable this feature in production</strong>
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#lossless-schema-update" target="_blank">Lossless Schema Updae</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder enableSchemaUpdate(boolean forceTableUpdate) {
            configMap.put(ENABLE_SCHEMA_UPDATE, forceTableUpdate);
            return this;
        }

        /**
         * Map to allow table schema update, same as
         * {@link #enableSchemaUpdate(boolean)} per table. <strong>It is strongly advised to disable this feature in production</strong>
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#lossless-schema-update" target="_blank">Lossless Schema Update</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder enableSchemaUpdateForTables(Map<String, Boolean> tables) {
            configMap.put(ENABLE_SCHEMA_UPDATE_FOR_TABLES, tables);
            return this;
        }

        /**
         * Define the pre-configured {@code com.datastax.driver.core.Session} object to
         * be used instead of creating a new one
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#native-session" target="_blank">Native Session</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withNativeSession(Session nativeSession) {
            configMap.put(NATIVE_SESSION, nativeSession);
            return this;
        }


        /**
         * Define the keyspace name to be used by Achilles. Note: you should
         * build as many PersistenceManagerFactory as different keyspaces to be
         * used
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#keyspace" target="_blank">Keyspace</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withKeyspaceName(String keyspaceName) {
            configMap.put(KEYSPACE_NAME, keyspaceName);
            return this;
        }

        /**
         * Provide a list of event interceptors
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#events-interceptors" target="_blank">Event interceptors</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withEventInterceptors(List<Interceptor<?>> interceptors) {
            configMap.put(EVENT_INTERCEPTORS, interceptors);
            return this;
        }

        /**
         * Activate Bean Validation (JSR303)
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#bean-validation" target="_blank">Bean validation</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder enableBeanValidation(boolean enableBeanValidation) {
            configMap.put(BEAN_VALIDATION_ENABLE, enableBeanValidation);
            return this;
        }

        /**
         * Provide custom validator for Bean Validation (JSR303)
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#bean-validation" target="_blank">Bean validation</a>
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
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#prepared-statements-cache" target="_blank">Prepared statements cache</a>
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder withMaxPreparedStatementCacheSize(int maxPreparedStatementCacheSize) {
            configMap.put(PREPARED_STATEMENTS_CACHE_SIZE, maxPreparedStatementCacheSize);
            return this;
        }

        /**
         * Whether to disable proxies warm up or not.
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#proxies" target="_blank">Proxies Warm Up</a>
         * @param disableProxiesWarmUp
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder disableProxiesWarmUp(boolean disableProxiesWarmUp) {
            configMap.put(PROXIES_WARM_UP_DISABLED, disableProxiesWarmUp);
            return this;
        }


        /**
         * Define the global insert strategy
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Insert-Strategy" target="_blank">Insert Strategy</a>
         * @param globalInsertStrategy
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder globalInsertStrategy(InsertStrategy globalInsertStrategy) {
            configMap.put(GLOBAL_INSERT_STRATEGY, globalInsertStrategy);
            return this;
        }

        /**
         * Whether to relax constraint on existing secondary indices validation
         *
         * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Configuration-Parameters#index-validation" target="_blank">Index Validation</a>
         * @param relaxIndexValidation
         *
         * @return PersistenceManagerFactoryBuilder
         */
        public PersistenceManagerFactoryBuilder relaxIndexValidation(boolean relaxIndexValidation) {
            configMap.put(RELAX_INDEX_VALIDATION, relaxIndexValidation);
            return this;
        }

        /**
         * Build a new PersistenceManagerFactory
         *
         * @return PersistenceManagerFactory
         */
        public PersistenceManagerFactory build() {
            return new PersistenceManagerFactory(cluster, configMap).bootstrap();
        }
    }
}
