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

package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static javax.validation.Validation.buildDefaultValidatorFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import javax.validation.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.async.DefaultExecutorThreadFactory;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internals.cache.StatementsCache;
import info.archinnov.achilles.internals.context.ConfigurationContext;
import info.archinnov.achilles.internals.factory.DefaultBeanFactory;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.types.ConfigMap;
import info.archinnov.achilles.json.DefaultJacksonMapperFactory;
import info.archinnov.achilles.json.JacksonMapperFactory;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.type.interceptor.Interceptor;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.strategy.NamingStrategy;

/**
 * Extract bootstrap argument and create a configuration context
 */
public class ArgumentExtractor {

    static final int DEFAULT_LRU_CACHE_SIZE = 10000;
    static final boolean DEFAULT_ENABLE_PRE_MUTATE_BEAN_VALIDATION = false;
    static final boolean DEFAULT_ENABLE_POST_LOAD_BEAN_VALIDATION = false;
    static final int DEFAULT_THREAD_POOL_MIN_THREAD_COUNT = 10;
    static final int DEFAULT_THREAD_POOL_MAX_THREAD_COUNT = 10;
    static final long DEFAULT_THREAD_POOL_THREAD_TTL = 60L;
    static final int DEFAULT_THREAD_POOL_QUEUE_SIZE = 1000;
    static final ThreadFactory DEFAULT_THREAD_POOL_THREAD_FACTORY = new DefaultExecutorThreadFactory();
    static final InsertStrategy DEFAULT_INSERT_STRATEGY = InsertStrategy.ALL_FIELDS;
    static final NamingStrategy DEFAULT_GLOBAL_NAMING_STRATEGY = NamingStrategy.LOWER_CASE;
    static final BeanFactory DEFAULT_BEAN_FACTORY = new DefaultBeanFactory();
    static final Integer DEFAULT_DML_RESULTS_DISPLAY_SIZE = 10;
    private static final Logger LOGGER = LoggerFactory.getLogger(ArgumentExtractor.class);

    public static ConfigurationContext initConfigContext(Cluster cluster, ConfigMap configurationMap) {
        LOGGER.trace("Build ConfigurationContext from configuration map");

        ConfigurationContext configContext = new ConfigurationContext();
        configContext.setCurrentKeyspace(initKeyspaceName(configurationMap));
        configContext.setForceSchemaGeneration(initForceSchemaCreation(configurationMap));
        configContext.setManageEntities(initManagedEntities(configurationMap));
        configContext.setJacksonMapperFactory(initObjectMapperFactory(configurationMap));
        configContext.setDefaultReadConsistencyLevel(initDefaultReadConsistencyLevel(configurationMap));
        configContext.setDefaultWriteConsistencyLevel(initDefaultWriteConsistencyLevel(configurationMap));
        configContext.setDefaultSerialConsistencyLevel(initDefaultSerialConsistencyLevel(configurationMap));
        configContext.setReadConsistencyLevelMap(initReadConsistencyMap(configurationMap));
        configContext.setWriteConsistencyLevelMap(initWriteConsistencyMap(configurationMap));
        configContext.setSerialConsistencyLevelMap(initSerialConsistencyMap(configurationMap));
        configContext.setBeanValidator(initValidator(configurationMap));
        configContext.setPostLoadBeanValidationEnabled(initPostLoadBeanValidation(configurationMap));
        configContext.setInterceptors(initInterceptors(configurationMap));
        configContext.setPreparedStatementLRUCacheSize(initPreparedStatementsCacheSize(configurationMap));
        configContext.setGlobalInsertStrategy(initInsertStrategy(configurationMap));
        configContext.setGlobalNamingStrategy(initGlobalNamingStrategy(configurationMap));
        configContext.setSchemaNameProvider(initSchemaNameProvider(configurationMap));
        configContext.setExecutorService(initExecutorService(configurationMap));
        configContext.setProvidedExecutorService(initProvidedExecutorService(configurationMap));
        configContext.setDefaultBeanFactory(initDefaultBeanFactory(configurationMap));
        configContext.setSession(initSession(cluster, configurationMap));
        configContext.setProvidedSession(initProvidedSession(configurationMap));
        configContext.setStatementsCache(initStatementCache(configurationMap));
        configContext.setRuntimeCodecs(initRuntimeCodecs(configurationMap));
        configContext.setValidateSchema(initValidateSchema(configurationMap));
        configContext.setDMLResultsDisplaySize(initDMLResultsDisplayLimit(configurationMap));
        return configContext;
    }

    static boolean initValidateSchema(ConfigMap configurationMap) {
        LOGGER.trace("Extract 'schema validation enabled' from configuration map");
        return configurationMap.getTypedOr(VALIDATE_SCHEMA, true);
    }

    static boolean initForceSchemaCreation(ConfigMap configurationMap) {
        LOGGER.trace("Extract 'force table creation' from configuration map");
        return configurationMap.getTypedOr(FORCE_SCHEMA_GENERATION, false);
    }


    static public List<Class<?>> initManagedEntities(ConfigMap configMap) {
        LOGGER.trace("Extract managed entity classes from configuration map");
        return configMap.getTypedOr(MANAGED_ENTITIES, new ArrayList<>());
    }

    static JacksonMapperFactory initObjectMapperFactory(ConfigMap configurationMap) {
        LOGGER.trace("Extract object mapper factory from configuration map");

        JacksonMapperFactory jacksonMapperFactory = configurationMap.getTyped(JACKSON_MAPPER_FACTORY);
        if (jacksonMapperFactory == null) {
            ObjectMapper mapper = configurationMap.getTyped(JACKSON_MAPPER);
            if (mapper != null) {
                jacksonMapperFactory = factoryFromMapper(mapper);
            } else {
                jacksonMapperFactory = new DefaultJacksonMapperFactory();
            }
        }

        return jacksonMapperFactory;
    }

    protected static JacksonMapperFactory factoryFromMapper(final ObjectMapper mapper) {
        return new JacksonMapperFactory() {
            @Override
            public <T> ObjectMapper getMapper(Class<T> type) {
                return mapper;
            }
        };
    }

    static Optional<ConsistencyLevel> initDefaultReadConsistencyLevel(ConfigMap configMap) {
        LOGGER.trace("Extract default read Consistency level from configuration map");
        return Optional.ofNullable(configMap.getTyped(CONSISTENCY_LEVEL_READ_DEFAULT));
    }

    static Optional<ConsistencyLevel> initDefaultWriteConsistencyLevel(ConfigMap configMap) {
        LOGGER.trace("Extract default read Consistency level from configuration map");
        return Optional.ofNullable(configMap.getTyped(CONSISTENCY_LEVEL_WRITE_DEFAULT));
    }

    static Optional<ConsistencyLevel> initDefaultSerialConsistencyLevel(ConfigMap configMap) {
        LOGGER.trace("Extract default write Consistency level from configuration map");
        return Optional.ofNullable(configMap.getTyped(CONSISTENCY_LEVEL_SERIAL_DEFAULT));
    }

    public static Map<String, ConsistencyLevel> initReadConsistencyMap(ConfigMap configMap) {
        LOGGER.trace("Extract read Consistency level map from configuration map");
        return configMap.getTypedOr(CONSISTENCY_LEVEL_READ_MAP, ImmutableMap.<String, ConsistencyLevel>of());
    }

    public static Map<String, ConsistencyLevel> initWriteConsistencyMap(ConfigMap configMap) {
        LOGGER.trace("Extract write Consistency level map from configuration map");
        return configMap.getTypedOr(CONSISTENCY_LEVEL_WRITE_MAP, ImmutableMap.<String, ConsistencyLevel>of());
    }

    public static Map<String, ConsistencyLevel> initSerialConsistencyMap(ConfigMap configMap) {
        LOGGER.trace("Extract serial Consistency level map from configuration map");
        return configMap.getTypedOr(CONSISTENCY_LEVEL_SERIAL_MAP, ImmutableMap.<String, ConsistencyLevel>of());
    }

    public static Optional<String> initKeyspaceName(ConfigMap configurationMap) {
        return Optional.ofNullable(configurationMap.<String>getTyped(KEYSPACE_NAME));
    }

    public static Session initSession(Cluster cluster, ConfigMap configurationMap) {
        LOGGER.trace("Extract or init Session from configuration map");

        return Optional.<Session>ofNullable(configurationMap.getTyped(NATIVE_SESSION))
                .orElseGet(() -> initKeyspaceName(configurationMap)
                                .map(cluster::connect)
                                .orElseGet(cluster::connect)
                );
    }

    public static boolean initProvidedSession(ConfigMap configurationMap) {
        LOGGER.trace("Is Session object provided or built internally ?");
        return Optional.<Session>ofNullable(configurationMap.getTyped(NATIVE_SESSION)).isPresent();
    }

    @SuppressWarnings("unchecked")
    public static List<Interceptor<?>> initInterceptors(ConfigMap configurationMap) {
        LOGGER.trace("Extract or init Interceptors");
        List<Interceptor<?>> interceptors = (List<Interceptor<?>>) configurationMap.get(EVENT_INTERCEPTORS);
        if (interceptors == null) {
            interceptors = new ArrayList<>();
        }
        return new ArrayList<>(new LinkedHashSet<>(interceptors));
    }

    static javax.validation.Validator initValidator(ConfigMap configurationMap) {
        LOGGER.trace("Extract or init Bean validation");
        Boolean enablePreMutateBeanValidation = configurationMap.getTypedOr(BEAN_VALIDATION_ENABLE, DEFAULT_ENABLE_PRE_MUTATE_BEAN_VALIDATION);
        if (enablePreMutateBeanValidation) {
            try {
                javax.validation.Validator defaultValidator = buildDefaultValidatorFactory().getValidator();
                return configurationMap.getTypedOr(BEAN_VALIDATION_VALIDATOR, defaultValidator);
            } catch (ValidationException vex) {
                throw new AchillesException("Cannot bootstrap ValidatorFactory for Bean Validation (JSR 303)", vex);
            }
        }
        return null;
    }

    static boolean initPostLoadBeanValidation(ConfigMap configMap) {
        LOGGER.trace("Extract or init Post Load Bean validation");
        return configMap.getTypedOr(POST_LOAD_BEAN_VALIDATION_ENABLE, DEFAULT_ENABLE_POST_LOAD_BEAN_VALIDATION);


    }

    public static Integer initPreparedStatementsCacheSize(ConfigMap configMap) {
        LOGGER.trace("Extract or init prepared statements cache size");
        return configMap.getTypedOr(PREPARED_STATEMENTS_CACHE_SIZE, DEFAULT_LRU_CACHE_SIZE);
    }

    public static InsertStrategy initInsertStrategy(ConfigMap configMap) {
        LOGGER.trace("Extract or init global Insert strategy");
        return configMap.getTypedOr(GLOBAL_INSERT_STRATEGY, DEFAULT_INSERT_STRATEGY);
    }

    public static NamingStrategy initGlobalNamingStrategy(ConfigMap configMap) {
        LOGGER.trace("Extract or init global Naming strategy");
        return configMap.getTypedOr(GLOBAL_NAMING_STRATEGY, DEFAULT_GLOBAL_NAMING_STRATEGY);
    }

    public static Optional<SchemaNameProvider> initSchemaNameProvider(ConfigMap configMap) {
        LOGGER.trace("Extract or init schema name provider");
        return Optional.ofNullable(configMap.getTyped(SCHEMA_NAME_PROVIDER));
    }

    public static ExecutorService initExecutorService(ConfigMap configMap) {
        LOGGER.trace("Extract or init executor service (thread pool)");
        return configMap.getTypedOr(EXECUTOR_SERVICE, initializeDefaultExecutor(configMap));
    }

    public static boolean initProvidedExecutorService(ConfigMap configMap) {
        LOGGER.trace("Is executor service provided or built internally ? ");
        return Optional.ofNullable(configMap.<ExecutorService>getTyped(EXECUTOR_SERVICE)).isPresent();
    }

    private static Supplier<ExecutorService> initializeDefaultExecutor(final ConfigMap configMap) {
        return () -> {
            int minThreads = configMap.getTypedOr(DEFAULT_EXECUTOR_SERVICE_MIN_THREAD, DEFAULT_THREAD_POOL_MIN_THREAD_COUNT);
            int maxThreads = configMap.getTypedOr(DEFAULT_EXECUTOR_SERVICE_MAX_THREAD, DEFAULT_THREAD_POOL_MAX_THREAD_COUNT);
            long threadKeepAlive = configMap.getTypedOr(DEFAULT_EXECUTOR_SERVICE_THREAD_KEEPALIVE, DEFAULT_THREAD_POOL_THREAD_TTL);
            int queueSize = configMap.getTypedOr(DEFAULT_EXECUTOR_SERVICE_QUEUE_SIZE, DEFAULT_THREAD_POOL_QUEUE_SIZE);
            ThreadFactory threadFactory = configMap.getTypedOr(DEFAULT_EXECUTOR_SERVICE_THREAD_FACTORY, DEFAULT_THREAD_POOL_THREAD_FACTORY);

            return new ThreadPoolExecutor(minThreads, maxThreads, threadKeepAlive, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(queueSize), threadFactory);
        };
    }

    private static BeanFactory initDefaultBeanFactory(final ConfigMap configMap) {
        LOGGER.trace("Extract or init default bean factory");
        if (configMap.containsKey(ConfigurationParameters.DEFAULT_BEAN_FACTORY)) {
            return configMap.<BeanFactory>getTyped(ConfigurationParameters.DEFAULT_BEAN_FACTORY);
        } else {
            return DEFAULT_BEAN_FACTORY;
        }
    }

    private static StatementsCache initStatementCache(final ConfigMap configMap) {
        LOGGER.trace("Extract or init default statement cache");
        if (configMap.containsKey(STATEMENTS_CACHE)) {
            return configMap.getTyped(STATEMENTS_CACHE);
        } else {
            final Integer cacheSize = initPreparedStatementsCacheSize(configMap);
            return new StatementsCache(cacheSize);
        }
    }

    private static Map<CodecSignature<?, ?>, Codec<?, ?>> initRuntimeCodecs(final ConfigMap configMap) {
        LOGGER.trace("Extract or init default runtime codecs");
        if (configMap.containsKey(RUNTIME_CODECS)) {
            return configMap.getTyped(RUNTIME_CODECS);
        } else {
            return new HashMap<>();
        }
    }

    private static Integer initDMLResultsDisplayLimit(final ConfigMap configMap) {
        if(configMap.containsKey(DML_RESULTS_DISPLAY_SIZE)) {
            final Integer resultsDisplaySize = configMap.getTyped(DML_RESULTS_DISPLAY_SIZE);
            return Integer.max(0,Integer.min(resultsDisplaySize, CassandraOptions.MAX_RESULTS_DISPLAY_SIZE));
        } else {
            return DEFAULT_DML_RESULTS_DISPLAY_SIZE;
        }
    }
}
