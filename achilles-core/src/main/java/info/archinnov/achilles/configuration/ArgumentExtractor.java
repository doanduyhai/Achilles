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

package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static javax.validation.Validation.buildDefaultValidatorFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.validation.ValidationException;
import org.apache.commons.lang3.StringUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.utils.ConfigMap;
import info.archinnov.achilles.json.DefaultJacksonMapperFactory;
import info.archinnov.achilles.json.JacksonMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;
import info.archinnov.achilles.type.NamingStrategy;

public class ArgumentExtractor {

    private static final Logger log = LoggerFactory.getLogger(ArgumentExtractor.class);

    static final ConsistencyLevel DEFAULT_LEVEL = ConsistencyLevel.ONE;

    static final int DEFAULT_LRU_CACHE_SIZE = 10000;

    static final boolean DEFAULT_ENABLE_BEAN_VALIDATION = false;

    static final boolean DEFAULT_PROXIES_WARM_UP_DISABLED = true;

    static final boolean DEFAULT_INDEX_RELAX_VALIDATION = false;

    static final int DEFAULT_THREAD_POOL_MIN_THREAD_COUNT = 10;
    static final int DEFAULT_THREAD_POOL_MAX_THREAD_COUNT = 10;
    static final long DEFAULT_THREAD_POOL_THREAD_TTL = 60L;
    static final int DEFAULT_THREAD_POOL_QUEUE_SIZE = 1000;

    static final InsertStrategy DEFAULT_INSERT_STRATEGY = InsertStrategy.ALL_FIELDS;
    static final NamingStrategy DEFAULT_GLOBAL_NAMING_STRATEGY = NamingStrategy.LOWER_CASE;

    public List<Class<?>> initEntities(ConfigMap configurationMap, ClassLoader classLoader) {
        log.trace("Extract entities from configuration map");

        List<String> entityPackages = getEntityPackages(configurationMap);
        List<Class<?>> entities = discoverEntities(entityPackages, classLoader);

        List<Class<?>> entitiesFromList = configurationMap.getTypedOr(ENTITIES_LIST, Collections.<Class<?>>emptyList());
        entities.addAll(entitiesFromList);
        return entities;
    }

    private List<String> getEntityPackages(ConfigMap configurationMap) {
        log.trace("Extract entity packages from configuration map");

        List<String> entityPackages = new ArrayList<>();
        String entityPackagesParameter = configurationMap.getTyped(ENTITY_PACKAGES);
        if (StringUtils.isNotBlank(entityPackagesParameter)) {
            entityPackages = Arrays.asList(StringUtils.split(entityPackagesParameter, ","));
        }

        return entityPackages;
    }

    private List<Class<?>> discoverEntities(List<String> packageNames, ClassLoader classLoader) {
        log.debug("Discovery of Achilles entity classes in packages {}", StringUtils.join(packageNames, ","));

        Set<Class<?>> candidateClasses = new HashSet<>();
        if (!packageNames.isEmpty()) {
            Reflections reflections = new Reflections(packageNames, classLoader);
            candidateClasses.addAll(reflections.getTypesAnnotatedWith(Entity.class));
        }
        return new ArrayList<>(candidateClasses);
    }

    public ConfigurationContext initConfigContext(ConfigMap configurationMap) {
        log.trace("Build ConfigurationContext from configuration map");

        ConfigurationContext configContext = new ConfigurationContext();
	configContext.setCurrentKeyspace(initKeyspaceName(configurationMap));
        configContext.setForceColumnFamilyCreation(initForceTableCreation(configurationMap));
        configContext.setEnableSchemaUpdate(initForceTableUpdate(configurationMap));
        configContext.setEnableSchemaUpdateForTables(initForceTableUpdateMap(configurationMap));
        configContext.setJacksonMapperFactory(initObjectMapperFactory(configurationMap));
        configContext.setDefaultReadConsistencyLevel(initDefaultReadConsistencyLevel(configurationMap));
        configContext.setDefaultWriteConsistencyLevel(initDefaultWriteConsistencyLevel(configurationMap));
        configContext.setReadConsistencyLevelMap(initReadConsistencyMap(configurationMap));
        configContext.setWriteConsistencyLevelMap(initWriteConsistencyMap(configurationMap));
        configContext.setBeanValidator(initValidator(configurationMap));
        configContext.setPreparedStatementLRUCacheSize(initPreparedStatementsCacheSize(configurationMap));
        configContext.setGlobalInsertStrategy(initInsertStrategy(configurationMap));
        configContext.setGlobalNamingStrategy(initGlobalNamingStrategy(configurationMap));
        configContext.setOSGIClassLoader(initOSGIClassLoader(configurationMap));
        configContext.setRelaxIndexValidation(initRelaxIndexValidation(configurationMap));
        configContext.setExecutorService(initExecutorService(configurationMap));
        return configContext;
    }

    boolean initForceTableCreation(ConfigMap configurationMap) {
        log.trace("Extract 'force table creation' from configuration map");
        return configurationMap.getTypedOr(FORCE_TABLE_CREATION, false);
    }

    boolean initForceTableUpdate(ConfigMap configurationMap) {
        log.trace("Extract 'force table update' from configuration map");

        return configurationMap.getTypedOr(ENABLE_SCHEMA_UPDATE, false);
    }

    public Map<String, Boolean> initForceTableUpdateMap(ConfigMap configMap) {
        log.trace("Extract 'force table update' map from configuration map");
        return configMap.getTypedOr(ENABLE_SCHEMA_UPDATE_FOR_TABLES, ImmutableMap.<String, Boolean>of());
    }

    JacksonMapperFactory initObjectMapperFactory(ConfigMap configurationMap) {
        log.trace("Extract object mapper factory from configuration map");

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

    ConsistencyLevel initDefaultReadConsistencyLevel(ConfigMap configMap) {
        log.trace("Extract default read Consistency level from configuration map");
        return configMap.getTypedOr(CONSISTENCY_LEVEL_READ_DEFAULT, DEFAULT_LEVEL);
    }

    ConsistencyLevel initDefaultWriteConsistencyLevel(ConfigMap configMap) {
        log.trace("Extract default write Consistency level from configuration map");
        return configMap.getTypedOr(CONSISTENCY_LEVEL_WRITE_DEFAULT, DEFAULT_LEVEL);
    }

    public Map<String, ConsistencyLevel> initReadConsistencyMap(ConfigMap configMap) {
        log.trace("Extract read Consistency level map from configuration map");
        return configMap.getTypedOr(CONSISTENCY_LEVEL_READ_MAP, ImmutableMap.<String, ConsistencyLevel>of());
    }

    public Map<String, ConsistencyLevel> initWriteConsistencyMap(ConfigMap configMap) {
        log.trace("Extract write Consistency level map from configuration map");
        return configMap.getTypedOr(CONSISTENCY_LEVEL_WRITE_MAP, ImmutableMap.<String, ConsistencyLevel>of());
    }

    public Optional<String> initKeyspaceName(ConfigMap configurationMap) {
        return Optional.fromNullable(configurationMap.<String>getTyped(KEYSPACE_NAME));
    }
    public Session initSession(Cluster cluster, ConfigMap configurationMap) {
        log.trace("Extract or init Session from configuration map");

        Session nativeSession = configurationMap.getTyped(NATIVE_SESSION);
        if (nativeSession == null) {
            final Optional<String> keyspaceNameO = initKeyspaceName(configurationMap);
            if (keyspaceNameO.isPresent()) {
                nativeSession = cluster.connect(keyspaceNameO.get());
            } else {
                nativeSession = cluster.connect();
            }
        }
        return nativeSession;
    }

    @SuppressWarnings("unchecked")
    public List<Interceptor<?>> initInterceptors(ConfigMap configurationMap) {

        List<Interceptor<?>> interceptors = (List<Interceptor<?>>) configurationMap.get(EVENT_INTERCEPTORS);
        if (interceptors == null) {
            interceptors = new ArrayList<>();
        }
        return new ArrayList<>(new LinkedHashSet<>(interceptors));
    }

    javax.validation.Validator initValidator(ConfigMap configurationMap) {
        Boolean enableBeanValidation = configurationMap.getTypedOr(BEAN_VALIDATION_ENABLE, DEFAULT_ENABLE_BEAN_VALIDATION);
        if (enableBeanValidation) {
            try {
                javax.validation.Validator defaultValidator = buildDefaultValidatorFactory().getValidator();
                return configurationMap.getTypedOr(BEAN_VALIDATION_VALIDATOR, defaultValidator);
            } catch (ValidationException vex) {
                throw new AchillesException("Cannot bootstrap ValidatorFactory for Bean Validation (JSR 303)", vex);
            }
        }
        return null;
    }

    public Integer initPreparedStatementsCacheSize(ConfigMap configMap) {
        return configMap.getTypedOr(PREPARED_STATEMENTS_CACHE_SIZE, DEFAULT_LRU_CACHE_SIZE);
    }

    public boolean initProxyWarmUp(ConfigMap configMap) {
        return configMap.getTypedOr(PROXIES_WARM_UP_DISABLED, DEFAULT_PROXIES_WARM_UP_DISABLED);
    }

    public InsertStrategy initInsertStrategy(ConfigMap configMap) {
        return configMap.getTypedOr(GLOBAL_INSERT_STRATEGY, DEFAULT_INSERT_STRATEGY);
    }

    public NamingStrategy initGlobalNamingStrategy(ConfigMap configMap) {
        return configMap.getTypedOr(GLOBAL_NAMING_STRATEGY, DEFAULT_GLOBAL_NAMING_STRATEGY);
    }

    public ClassLoader initOSGIClassLoader(ConfigMap configMap) {
        return configMap.getTyped(OSGI_CLASS_LOADER);
    }

    public boolean initRelaxIndexValidation(ConfigMap configMap) {
        return configMap.getTypedOr(RELAX_INDEX_VALIDATION, DEFAULT_INDEX_RELAX_VALIDATION);
    }

    public ExecutorService initExecutorService(ConfigMap configMap) {
        return configMap.getTypedOr(EXECUTOR_SERVICE, initializeDefaultExecutor(configMap));
    }

    private ExecutorService initializeDefaultExecutor(ConfigMap configMap) {

        int minThreads = configMap.getTypedOr(DEFAULT_EXECUTOR_SERVICE_MIN_THREAD, DEFAULT_THREAD_POOL_MIN_THREAD_COUNT);
        int maxThreads = configMap.getTypedOr(DEFAULT_EXECUTOR_SERVICE_MAX_THREAD, DEFAULT_THREAD_POOL_MAX_THREAD_COUNT);
        long threadKeepAlive = configMap.getTypedOr(DEFAULT_EXECUTOR_SERVICE_THREAD_KEEPALIVE, DEFAULT_THREAD_POOL_THREAD_TTL);
        int queueSize = configMap.getTypedOr(DEFAULT_EXECUTOR_SERVICE_QUEUE_SIZE, DEFAULT_THREAD_POOL_QUEUE_SIZE);
        return new ThreadPoolExecutor(minThreads, maxThreads, threadKeepAlive, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(queueSize),
                new DefaultExecutorThreadFactory());
    }
}
