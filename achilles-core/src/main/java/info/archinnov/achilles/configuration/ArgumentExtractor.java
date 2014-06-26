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
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_BATCH_STATEMENTS_ORDERING;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.INSERT_STRATEGY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.configuration.ConfigurationParameters.NATIVE_SESSION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_FACTORY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OSGI_CLASS_LOADER;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PREPARED_STATEMENTS_CACHE_SIZE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PROXIES_WARM_UP_DISABLED;
import static javax.validation.Validation.buildDefaultValidatorFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.validation.ValidationException;
import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.utils.ConfigMap;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.json.DefaultObjectMapperFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;

public class ArgumentExtractor {

    private static final Logger log = LoggerFactory.getLogger(ArgumentExtractor.class);

    static final ConsistencyLevel DEFAULT_LEVEL = ConsistencyLevel.ONE;

    static final int DEFAULT_LRU_CACHE_SIZE = 10000;

    static final boolean DEFAULT_ENABLE_BEAN_VALIDATION = false;

    static final boolean DEFAULT_PROXIES_WARM_UP_DISABLED = true;

    static final boolean DEFAULT_FORCE_BATCH_STATEMENTS_ORDERING = false;

    static final InsertStrategy DEFAULT_INSERT_STRATEGY = InsertStrategy.ALL_FIELDS;


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
        configContext.setForceColumnFamilyCreation(initForceTableCreation(configurationMap));
        configContext.setEnableSchemaUpdate(initForceTableUpdate(configurationMap));
        configContext.setEnableSchemaUpdateForTables(initForceTableUpdateMap(configurationMap));
        configContext.setObjectMapperFactory(initObjectMapperFactory(configurationMap));
        configContext.setDefaultReadConsistencyLevel(initDefaultReadConsistencyLevel(configurationMap));
        configContext.setDefaultWriteConsistencyLevel(initDefaultWriteConsistencyLevel(configurationMap));
        configContext.setBeanValidator(initValidator(configurationMap));
        configContext.setPreparedStatementLRUCacheSize(initPreparedStatementsCacheSize(configurationMap));
        configContext.setForceBatchStatementsOrdering(initForceBatchStatementsOrdering(configurationMap));
        configContext.setInsertStrategy(initInsertStrategy(configurationMap));
        configContext.setOSGIClassLoader(initOSGIClassLoader(configurationMap));
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

    ObjectMapperFactory initObjectMapperFactory(ConfigMap configurationMap) {
        log.trace("Extract object mapper factory from configuration map");

        ObjectMapperFactory objectMapperFactory = configurationMap.getTyped(OBJECT_MAPPER_FACTORY);
        if (objectMapperFactory == null) {
            ObjectMapper mapper = configurationMap.getTyped(OBJECT_MAPPER);
            if (mapper != null) {
                objectMapperFactory = factoryFromMapper(mapper);
            } else {
                objectMapperFactory = new DefaultObjectMapperFactory();
            }
        }

        return objectMapperFactory;
    }

    protected static ObjectMapperFactory factoryFromMapper(final ObjectMapper mapper) {
        return new ObjectMapperFactory() {
            @Override
            public <T> ObjectMapper getMapper(Class<T> type) {
                return mapper;
            }
        };
    }

    ConsistencyLevel initDefaultReadConsistencyLevel(ConfigMap configMap) {
        log.trace("Extract default read Consistency level from configuration map");

        String defaultReadLevel = configMap.getTyped(CONSISTENCY_LEVEL_READ_DEFAULT);
        return parseConsistencyLevelOrGetDefault(defaultReadLevel);
    }

    ConsistencyLevel initDefaultWriteConsistencyLevel(ConfigMap configMap) {
        log.trace("Extract default write Consistency level from configuration map");

        String defaultWriteLevel = configMap.getTyped(CONSISTENCY_LEVEL_WRITE_DEFAULT);
        return parseConsistencyLevelOrGetDefault(defaultWriteLevel);
    }

    public Map<String, ConsistencyLevel> initReadConsistencyMap(ConfigMap configMap) {
        log.trace("Extract read Consistency level map from configuration map");

        Map<String, String> readConsistencyMap = configMap.getTyped(CONSISTENCY_LEVEL_READ_MAP);

        return parseConsistencyLevelMap(readConsistencyMap);
    }

    public Map<String, ConsistencyLevel> initWriteConsistencyMap(ConfigMap configMap) {
        log.trace("Extract write Consistency level map from configuration map");

        Map<String, String> writeConsistencyMap = configMap.getTyped(CONSISTENCY_LEVEL_WRITE_MAP);

        return parseConsistencyLevelMap(writeConsistencyMap);
    }

    public Session initSession(Cluster cluster, ConfigMap configurationMap) {
        log.trace("Extract or init Session from configuration map");

        Session nativeSession = configurationMap.getTyped(NATIVE_SESSION);
        String keyspace = configurationMap.getTyped(KEYSPACE_NAME);
        Validator.validateNotBlank(keyspace, "%s property should be provided", KEYSPACE_NAME);

        if (nativeSession == null) {
            nativeSession = cluster.connect(keyspace);
        }
        return nativeSession;
    }

    private Map<String, ConsistencyLevel> parseConsistencyLevelMap(Map<String, String> consistencyLevelMap) {
        log.trace("Extract read Consistency level map from configuration map");

        Map<String, ConsistencyLevel> map = new HashMap<>();
        if (consistencyLevelMap != null && !consistencyLevelMap.isEmpty()) {
            for (Entry<String, String> entry : consistencyLevelMap.entrySet()) {
                map.put(entry.getKey(), parseConsistencyLevelOrGetDefault(entry.getValue()));
            }
        }

        return map;
    }

    private ConsistencyLevel parseConsistencyLevelOrGetDefault(String consistencyLevel) {
        ConsistencyLevel level = DEFAULT_LEVEL;
        if (StringUtils.isNotBlank(consistencyLevel)) {
            try {
                level = ConsistencyLevel.valueOf(consistencyLevel);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("'" + consistencyLevel + "' is not a valid Consistency Level");
            }
        }
        return level;
    }

    @SuppressWarnings("unchecked")
    public List<Interceptor<?>> initInterceptors(ConfigMap configurationMap) {

        List<Interceptor<?>> interceptors = (List<Interceptor<?>>) configurationMap.get(EVENT_INTERCEPTORS);
        if (interceptors == null) {
            interceptors = new ArrayList<>();
        }
        return interceptors;
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

    public boolean initForceBatchStatementsOrdering(ConfigMap configMap) {
        return configMap.getTypedOr(FORCE_BATCH_STATEMENTS_ORDERING, DEFAULT_FORCE_BATCH_STATEMENTS_ORDERING);
    }

    public InsertStrategy initInsertStrategy(ConfigMap configMap) {
        return configMap.getTypedOr(INSERT_STRATEGY, DEFAULT_INSERT_STRATEGY);
    }

    public ClassLoader initOSGIClassLoader(ConfigMap configMap) {
        return configMap.getTyped(OSGI_CLASS_LOADER);
    }
}
