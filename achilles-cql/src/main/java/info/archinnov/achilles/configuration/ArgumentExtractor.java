/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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

import static info.archinnov.achilles.configuration.ConfigurationParameters.CLUSTER_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.COMPRESSION_TYPE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONNECTION_CONTACT_POINTS_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONNECTION_PORT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.DISABLE_JMX;
import static info.archinnov.achilles.configuration.ConfigurationParameters.DISABLE_METRICS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.LOAD_BALANCING_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.NATIVE_SESSION_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PASSWORD;
import static info.archinnov.achilles.configuration.ConfigurationParameters.RECONNECTION_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.RETRY_POLICY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.SSL_ENABLED;
import static info.archinnov.achilles.configuration.ConfigurationParameters.SSL_OPTIONS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.USERNAME;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_DEFAULT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_MAP_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_MAP_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.DEFAULT_LEVEL;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_CF_CREATION_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_FACTORY_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_PARAM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import info.archinnov.achilles.json.DefaultObjectMapperFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

public class ArgumentExtractor {

	public List<String> initEntityPackages(Map<String, Object> configurationMap) {
		List<String> entityPackages = new ArrayList<String>();
		String entityPackagesParameter = (String) configurationMap.get(ENTITY_PACKAGES_PARAM);
		if (StringUtils.isNotBlank(entityPackagesParameter)) {
			entityPackages = Arrays.asList(StringUtils.split(entityPackagesParameter, ","));
		}

		return entityPackages;
	}

	public boolean initForceCFCreation(Map<String, Object> configurationMap) {
		Boolean forceColumnFamilyCreation = (Boolean) configurationMap.get(FORCE_CF_CREATION_PARAM);
		if (forceColumnFamilyCreation != null) {
			return forceColumnFamilyCreation;
		} else {
			return false;
		}
	}

	public ObjectMapperFactory initObjectMapperFactory(Map<String, Object> configurationMap) {
		ObjectMapperFactory objectMapperFactory = (ObjectMapperFactory) configurationMap
				.get(OBJECT_MAPPER_FACTORY_PARAM);
		if (objectMapperFactory == null) {
			ObjectMapper mapper = (ObjectMapper) configurationMap.get(OBJECT_MAPPER_PARAM);
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

	public ConsistencyLevel initDefaultReadConsistencyLevel(Map<String, Object> configMap) {
		String defaultReadLevel = (String) configMap.get(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultReadLevel);
	}

	public ConsistencyLevel initDefaultWriteConsistencyLevel(Map<String, Object> configMap) {
		String defaultWriteLevel = (String) configMap.get(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultWriteLevel);
	}

	public Map<String, ConsistencyLevel> initReadConsistencyMap(Map<String, Object> configMap) {
		@SuppressWarnings("unchecked")
		Map<String, String> readConsistencyMap = (Map<String, String>) configMap.get(CONSISTENCY_LEVEL_READ_MAP_PARAM);

		return parseConsistencyLevelMap(readConsistencyMap);
	}

	public Map<String, ConsistencyLevel> initWriteConsistencyMap(Map<String, Object> configMap) {
		@SuppressWarnings("unchecked")
		Map<String, String> writeConsistencyMap = (Map<String, String>) configMap
				.get(CONSISTENCY_LEVEL_WRITE_MAP_PARAM);

		return parseConsistencyLevelMap(writeConsistencyMap);
	}

    public Cluster initCluster(Map<String, Object> configurationMap) {
        Cluster cluster = (Cluster) configurationMap.get(CLUSTER_PARAM);
        if (cluster == null) {
            String contactPoints = (String) configurationMap.get(CONNECTION_CONTACT_POINTS_PARAM);
            Integer port = (Integer) configurationMap.get(CONNECTION_PORT_PARAM);

            ProtocolOptions.Compression compression = ProtocolOptions.Compression.SNAPPY;
            if (configurationMap.containsKey(COMPRESSION_TYPE)) {
                compression = (ProtocolOptions.Compression) configurationMap.get(COMPRESSION_TYPE);
            }

            RetryPolicy retryPolicy = Policies.defaultRetryPolicy();
            if (configurationMap.containsKey(RETRY_POLICY)) {
                retryPolicy = (RetryPolicy) configurationMap.get(RETRY_POLICY);
            }

            LoadBalancingPolicy loadBalancingPolicy = Policies.defaultLoadBalancingPolicy();
            if (configurationMap.containsKey(LOAD_BALANCING_POLICY)) {
                loadBalancingPolicy = (LoadBalancingPolicy) configurationMap.get(LOAD_BALANCING_POLICY);
            }

            ReconnectionPolicy reconnectionPolicy = Policies.defaultReconnectionPolicy();
            if (configurationMap.containsKey(RECONNECTION_POLICY)) {
                reconnectionPolicy = (ReconnectionPolicy) configurationMap.get(RECONNECTION_POLICY);
            }

            String username = null;
            String password = null;
            if (configurationMap.containsKey(USERNAME) && configurationMap.containsKey(PASSWORD)) {
                username = (String) configurationMap.get(USERNAME);
                password = (String) configurationMap.get(PASSWORD);
            }

            boolean disableJmx = false;
            if (configurationMap.containsKey(DISABLE_JMX)) {
                disableJmx = (Boolean) configurationMap.get(DISABLE_JMX);
            }

            boolean disableMetrics = false;
            if (configurationMap.containsKey(DISABLE_METRICS)) {
                disableMetrics = (Boolean) configurationMap.get(DISABLE_METRICS);
            }

            boolean sslEnabled = false;
            if (configurationMap.containsKey(SSL_ENABLED)) {
                sslEnabled = (Boolean) configurationMap.get(SSL_ENABLED);
            }

            SSLOptions sslOptions = null;
            if (configurationMap.containsKey(SSL_OPTIONS)) {
                sslOptions = (SSLOptions) configurationMap.get(SSL_OPTIONS);
            }

            Validator
                    .validateNotBlank(contactPoints, "%s property should be provided", CONNECTION_CONTACT_POINTS_PARAM);
            Validator.validateNotNull(port, "%s property should be provided", CONNECTION_PORT_PARAM);
            if (sslEnabled) {
                Validator
                        .validateNotNull(sslOptions, "%s property should be provided when SSL is enabled", SSL_OPTIONS);
            }

            String[] contactPointsList = StringUtils.split(contactPoints, ",");

            Cluster.Builder clusterBuilder = Cluster.builder().addContactPoints(contactPointsList).withPort(port)
                                                    .withCompression(compression).withRetryPolicy(retryPolicy)
                                                    .withLoadBalancingPolicy(loadBalancingPolicy).withReconnectionPolicy(reconnectionPolicy);

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

    public Session initSession(Cluster cluster, Map<String, Object> configurationMap) {

        Session nativeSession = (Session) configurationMap.get(NATIVE_SESSION_PARAM);
        String keyspace = (String) configurationMap.get(KEYSPACE_NAME_PARAM);
        Validator.validateNotBlank(keyspace, "%s property should be provided", KEYSPACE_NAME_PARAM);

        if (nativeSession == null) {
            nativeSession = cluster.connect(keyspace);
        }
        return nativeSession;
    }

	private Map<String, ConsistencyLevel> parseConsistencyLevelMap(Map<String, String> consistencyLevelMap) {
		Map<String, ConsistencyLevel> map = new HashMap<String, ConsistencyLevel>();
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
}
