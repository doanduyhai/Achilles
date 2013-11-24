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

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.json.DefaultObjectMapperFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
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

public class ArgumentExtractor {

    private static final Logger log  = LoggerFactory.getLogger(ArgumentExtractor.class);

	public List<String> initEntityPackages(Map<String, Object> configurationMap) {
        log.trace("Extract entity packages from configuration map");

		List<String> entityPackages = new ArrayList<String>();
		String entityPackagesParameter = (String) configurationMap.get(ENTITY_PACKAGES_PARAM);
		if (StringUtils.isNotBlank(entityPackagesParameter)) {
			entityPackages = Arrays.asList(StringUtils.split(entityPackagesParameter, ","));
		}

		return entityPackages;
	}

	public ConfigurationContext initConfigContext(Map<String, Object> configurationMap) {
        log.trace("Build ConfigurationContext from configuration map");

		ConfigurationContext configContext = new ConfigurationContext();
		configContext.setForceColumnFamilyCreation(initForceTableCreation(configurationMap));
		configContext.setObjectMapperFactory(initObjectMapperFactory(configurationMap));
		configContext.setDefaultReadConsistencyLevel(initDefaultReadConsistencyLevel(configurationMap));
		configContext.setDefaultWriteConsistencyLevel(initDefaultWriteConsistencyLevel(configurationMap));
		return configContext;
	}

	boolean initForceTableCreation(Map<String, Object> configurationMap) {
        log.trace("Extract 'force table creation' from configuration map");

		Boolean forceColumnFamilyCreation = (Boolean) configurationMap.get(FORCE_TABLE_CREATION_PARAM);
		if (forceColumnFamilyCreation != null) {
			return forceColumnFamilyCreation;
		} else {
			return false;
		}
	}

	ObjectMapperFactory initObjectMapperFactory(Map<String, Object> configurationMap) {
        log.trace("Extract object mapper factory from configuration map");

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

	ConsistencyLevel initDefaultReadConsistencyLevel(Map<String, Object> configMap) {
        log.trace("Extract default read Consistency level from configuration map");

		String defaultReadLevel = (String) configMap.get(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultReadLevel);
	}

	ConsistencyLevel initDefaultWriteConsistencyLevel(Map<String, Object> configMap) {
        log.trace("Extract default write Consistency level from configuration map");

		String defaultWriteLevel = (String) configMap.get(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultWriteLevel);
	}

	public Map<String, ConsistencyLevel> initReadConsistencyMap(Map<String, Object> configMap) {
        log.trace("Extract read Consistency level map from configuration map");

		@SuppressWarnings("unchecked")
		Map<String, String> readConsistencyMap = (Map<String, String>) configMap.get(CONSISTENCY_LEVEL_READ_MAP_PARAM);

		return parseConsistencyLevelMap(readConsistencyMap);
	}

	public Map<String, ConsistencyLevel> initWriteConsistencyMap(Map<String, Object> configMap) {
        log.trace("Extract write Consistency level map from configuration map");

		@SuppressWarnings("unchecked")
		Map<String, String> writeConsistencyMap = (Map<String, String>) configMap
				.get(CONSISTENCY_LEVEL_WRITE_MAP_PARAM);

		return parseConsistencyLevelMap(writeConsistencyMap);
	}

	public Cluster initCluster(Map<String, Object> configurationMap) {
        log.trace("Extract or init cluster from configuration map");

		Cluster cluster = (Cluster) configurationMap.get(CLUSTER_PARAM);
		if (cluster == null) {
			String contactPoints = (String) configurationMap.get(CONNECTION_CONTACT_POINTS_PARAM);
			Integer port = (Integer) configurationMap.get(CONNECTION_CQL_PORT_PARAM);

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
			Validator.validateNotNull(port, "%s property should be provided", CONNECTION_CQL_PORT_PARAM);
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
        log.trace("Extract or init Session from configuration map");

		Session nativeSession = (Session) configurationMap.get(NATIVE_SESSION_PARAM);
		String keyspace = (String) configurationMap.get(KEYSPACE_NAME_PARAM);
		Validator.validateNotBlank(keyspace, "%s property should be provided", KEYSPACE_NAME_PARAM);

		if (nativeSession == null) {
			nativeSession = cluster.connect(keyspace);
		}
		return nativeSession;
	}

	private Map<String, ConsistencyLevel> parseConsistencyLevelMap(Map<String, String> consistencyLevelMap) {
        log.trace("Extract read Consistency level map from configuration map");

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
