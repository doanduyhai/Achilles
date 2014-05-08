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
import static info.archinnov.achilles.configuration.ConfigurationParameters.CLUSTER_NAME_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CLUSTER_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.COMPRESSION_TYPE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONNECTION_CONTACT_POINTS_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONNECTION_CQL_PORT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_DEFAULT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_MAP_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_MAP_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.DEFAULT_LEVEL;
import static info.archinnov.achilles.configuration.ConfigurationParameters.DISABLE_JMX;
import static info.archinnov.achilles.configuration.ConfigurationParameters.DISABLE_METRICS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITIES_LIST_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.EVENT_INTERCEPTORS_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_BATCH_STATEMENTS_ORDERING;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.INSERT_STRATEGY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.InsertStrategy;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.LOAD_BALANCING_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.NATIVE_SESSION_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_FACTORY_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PASSWORD;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PREPARED_STATEMENTS_CACHE_SIZE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PROXIES_WARM_UP_DISABLED;
import static info.archinnov.achilles.configuration.ConfigurationParameters.RECONNECTION_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.RETRY_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.SSL_ENABLED;
import static info.archinnov.achilles.configuration.ConfigurationParameters.SSL_OPTIONS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.USERNAME;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.json.DefaultObjectMapperFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.TypedMap;

public class ArgumentExtractor {

    private static final Logger log = LoggerFactory.getLogger(ArgumentExtractor.class);

    static final int DEFAULT_LRU_CACHE_SIZE = 10000;

    static final boolean DEFAULT_ENABLE_BEAN_VALIDATION = false;

    static final boolean DEFAULT_PROXIES_WARM_UP_DISABLED = true;

    static final boolean DEFAULT_FORCE_BATCH_STATEMENTS_ORDERING = false;

    static final InsertStrategy DEFAULT_INSERT_STRATEGY = InsertStrategy.ALL_FIELDS;


    public List<Class<?>> initEntities(TypedMap configurationMap) {
        log.trace("Extract entities from configuration map");

        List<String> entityPackages = getEntityPackages(configurationMap);
        List<Class<?>> entities = discoverEntities(entityPackages);

        List<Class<?>> entitiesFromList = configurationMap.getTypedOr(ENTITIES_LIST_PARAM, Collections.<Class<?>>emptyList());
        entities.addAll(entitiesFromList);
        return entities;
    }

    private List<String> getEntityPackages(TypedMap configurationMap) {
        log.trace("Extract entity packages from configuration map");

        List<String> entityPackages = new ArrayList<>();
        String entityPackagesParameter = configurationMap.getTyped(ENTITY_PACKAGES_PARAM);
        if (StringUtils.isNotBlank(entityPackagesParameter)) {
            entityPackages = Arrays.asList(StringUtils.split(entityPackagesParameter, ","));
        }

        return entityPackages;
    }

    private List<Class<?>> discoverEntities(List<String> packageNames) {
        log.debug("Discovery of Achilles entity classes in packages {}", StringUtils.join(packageNames, ","));

        Set<Class<?>> candidateClasses = new HashSet<>();
        if (!packageNames.isEmpty()) {
            Reflections reflections = new Reflections(packageNames);
            candidateClasses.addAll(reflections.getTypesAnnotatedWith(Entity.class));
        }
        return new ArrayList<>(candidateClasses);
    }

    public ConfigurationContext initConfigContext(TypedMap configurationMap) {
        log.trace("Build ConfigurationContext from configuration map");

        ConfigurationContext configContext = new ConfigurationContext();
        configContext.setForceColumnFamilyCreation(initForceTableCreation(configurationMap));
        configContext.setObjectMapperFactory(initObjectMapperFactory(configurationMap));
        configContext.setDefaultReadConsistencyLevel(initDefaultReadConsistencyLevel(configurationMap));
        configContext.setDefaultWriteConsistencyLevel(initDefaultWriteConsistencyLevel(configurationMap));
        configContext.setBeanValidator(initValidator(configurationMap));
        configContext.setPreparedStatementLRUCacheSize(initPreparedStatementsCacheSize(configurationMap));
        configContext.setForceBatchStatementsOrdering(initForceBatchStatementsOrdering(configurationMap));
        configContext.setInsertStrategy(initInsertStrategy(configurationMap));
        return configContext;
    }

    boolean initForceTableCreation(TypedMap configurationMap) {
        log.trace("Extract 'force table creation' from configuration map");

        Boolean forceColumnFamilyCreation = configurationMap.getTyped(FORCE_TABLE_CREATION_PARAM);
        if (forceColumnFamilyCreation != null) {
            return forceColumnFamilyCreation;
        } else {
            return false;
        }
    }

    ObjectMapperFactory initObjectMapperFactory(TypedMap configurationMap) {
        log.trace("Extract object mapper factory from configuration map");

        ObjectMapperFactory objectMapperFactory = configurationMap.getTyped(OBJECT_MAPPER_FACTORY_PARAM);
        if (objectMapperFactory == null) {
            ObjectMapper mapper = configurationMap.getTyped(OBJECT_MAPPER_PARAM);
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

    ConsistencyLevel initDefaultReadConsistencyLevel(TypedMap configMap) {
        log.trace("Extract default read Consistency level from configuration map");

        String defaultReadLevel = configMap.getTyped(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM);
        return parseConsistencyLevelOrGetDefault(defaultReadLevel);
    }

    ConsistencyLevel initDefaultWriteConsistencyLevel(TypedMap configMap) {
        log.trace("Extract default write Consistency level from configuration map");

        String defaultWriteLevel = configMap.getTyped(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM);
        return parseConsistencyLevelOrGetDefault(defaultWriteLevel);
    }

    public Map<String, ConsistencyLevel> initReadConsistencyMap(TypedMap configMap) {
        log.trace("Extract read Consistency level map from configuration map");

        Map<String, String> readConsistencyMap = configMap.getTyped(CONSISTENCY_LEVEL_READ_MAP_PARAM);

        return parseConsistencyLevelMap(readConsistencyMap);
    }

    public Map<String, ConsistencyLevel> initWriteConsistencyMap(TypedMap configMap) {
        log.trace("Extract write Consistency level map from configuration map");

        Map<String, String> writeConsistencyMap = configMap.getTyped(CONSISTENCY_LEVEL_WRITE_MAP_PARAM);

        return parseConsistencyLevelMap(writeConsistencyMap);
    }

    public Cluster initCluster(TypedMap configurationMap) {
        log.trace("Extract or init cluster from configuration map");

        Cluster cluster = configurationMap.getTyped(CLUSTER_PARAM);
        if (cluster == null) {
            String contactPoints = configurationMap.getTyped(CONNECTION_CONTACT_POINTS_PARAM);
            Integer port = configurationMap.getTyped(CONNECTION_CQL_PORT_PARAM);

            ProtocolOptions.Compression compression = ProtocolOptions.Compression.SNAPPY;
            if (configurationMap.containsKey(COMPRESSION_TYPE)) {
                compression = configurationMap.getTyped(COMPRESSION_TYPE);
            }

            RetryPolicy retryPolicy = Policies.defaultRetryPolicy();
            if (configurationMap.containsKey(RETRY_POLICY)) {
                retryPolicy = configurationMap.getTyped(RETRY_POLICY);
            }

            LoadBalancingPolicy loadBalancingPolicy = Policies.defaultLoadBalancingPolicy();
            if (configurationMap.containsKey(LOAD_BALANCING_POLICY)) {
                loadBalancingPolicy = configurationMap.getTyped(LOAD_BALANCING_POLICY);
            }

            ReconnectionPolicy reconnectionPolicy = Policies.defaultReconnectionPolicy();
            if (configurationMap.containsKey(RECONNECTION_POLICY)) {
                reconnectionPolicy = configurationMap.getTyped(RECONNECTION_POLICY);
            }

            String username = null;
            String password = null;
            if (configurationMap.containsKey(USERNAME) && configurationMap.containsKey(PASSWORD)) {
                username = configurationMap.getTyped(USERNAME);
                password = configurationMap.getTyped(PASSWORD);
            }

            boolean disableJmx = false;
            if (configurationMap.containsKey(DISABLE_JMX)) {
                disableJmx = configurationMap.getTyped(DISABLE_JMX);
            }

            boolean disableMetrics = false;
            if (configurationMap.containsKey(DISABLE_METRICS)) {
                disableMetrics = configurationMap.getTyped(DISABLE_METRICS);
            }

            boolean sslEnabled = false;
            if (configurationMap.containsKey(SSL_ENABLED)) {
                sslEnabled = configurationMap.getTyped(SSL_ENABLED);
            }

            SSLOptions sslOptions = null;
            if (configurationMap.containsKey(SSL_OPTIONS)) {
                sslOptions = configurationMap.getTyped(SSL_OPTIONS);
            }

            String clusterName = null;
            if (configurationMap.containsKey(CLUSTER_NAME_PARAM)) {
                clusterName = configurationMap.getTyped(CLUSTER_NAME_PARAM);
            }

            Validator
                    .validateNotBlank(contactPoints, "%s property should be provided",
                            CONNECTION_CONTACT_POINTS_PARAM);
            Validator.validateNotNull(port, "%s property should be provided", CONNECTION_CQL_PORT_PARAM);
            if (sslEnabled) {
                Validator
                        .validateNotNull(sslOptions, "%s property should be provided when SSL is enabled",
                                SSL_OPTIONS);
            }

            String[] contactPointsList = StringUtils.split(contactPoints, ",");

            Cluster.Builder clusterBuilder = Cluster.builder().addContactPoints(contactPointsList).withPort(port)
                    .withCompression(compression).withRetryPolicy(retryPolicy)
                    .withLoadBalancingPolicy(loadBalancingPolicy)
                    .withReconnectionPolicy(reconnectionPolicy);

            if (StringUtils.isNotBlank(clusterName)) {
                clusterBuilder.withClusterName(clusterName);
            }
            if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
                clusterBuilder.withCredentials(username, password);
            }

            if (disableJmx) {
                clusterBuilder.withoutJMXReporting();
            }

            if (disableMetrics) {
                clusterBuilder.withoutMetrics();
            }

            if (sslEnabled) {
                clusterBuilder.withSSL().withSSL(sslOptions);
            }
            cluster = clusterBuilder.build();
        }
        return cluster;
    }

    public Session initSession(Cluster cluster, TypedMap configurationMap) {
        log.trace("Extract or init Session from configuration map");

        Session nativeSession = configurationMap.getTyped(NATIVE_SESSION_PARAM);
        String keyspace = configurationMap.getTyped(KEYSPACE_NAME_PARAM);
        Validator.validateNotBlank(keyspace, "%s property should be provided", KEYSPACE_NAME_PARAM);

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
    public List<Interceptor<?>> initInterceptors(TypedMap configurationMap) {

        List<Interceptor<?>> interceptors = (List<Interceptor<?>>) configurationMap.get(EVENT_INTERCEPTORS_PARAM);
        if (interceptors == null) {
            interceptors = new ArrayList<>();
        }
        return interceptors;
    }

    javax.validation.Validator initValidator(TypedMap configurationMap) {
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

    public Integer initPreparedStatementsCacheSize(TypedMap configMap) {
        return configMap.getTypedOr(PREPARED_STATEMENTS_CACHE_SIZE, DEFAULT_LRU_CACHE_SIZE);
    }

    public boolean initProxyWarmUp(TypedMap configMap) {
        return configMap.getTypedOr(PROXIES_WARM_UP_DISABLED, DEFAULT_PROXIES_WARM_UP_DISABLED);
    }

    public boolean initForceBatchStatementsOrdering(TypedMap configMap) {
        return configMap.getTypedOr(FORCE_BATCH_STATEMENTS_ORDERING, DEFAULT_FORCE_BATCH_STATEMENTS_ORDERING);
    }

    public InsertStrategy initInsertStrategy(TypedMap configMap) {
        return configMap.getTypedOr(INSERT_STRATEGY, DEFAULT_INSERT_STRATEGY);
    }
}
