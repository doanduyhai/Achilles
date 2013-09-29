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
package info.archinnov.achilles.integration.spring;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static org.apache.commons.lang.StringUtils.*;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManagerFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

@Configuration
public class CQLEntityManagerJavaConfigSample {

	@Value("#{cassandraProperties['achilles.entity.packages']}")
	private String entityPackages;

	@Value("#{cassandraProperties['achilles.cassandra.connection.contactPoints']}")
	private String contactPoints;

	@Value("#{cassandraProperties['achilles.cassandra.connection.port']}")
	private Integer port;

	@Value("#{cassandraProperties['achilles.cassandra.keyspace.name']}")
	private String keyspaceName;

	@Autowired
	private RetryPolicy retryPolicy;

	@Autowired
	private LoadBalancingPolicy loadBalancingPolicy;

	@Autowired
	private ReconnectionPolicy reconnectionPolicy;

	@Value("#{cassandraProperties['achilles.cassandra.username']}")
	private String username;

	@Value("#{cassandraProperties['achilles.cassandra.password']}")
	private String password;

	@Value("#{cassandraProperties['achilles.cassandra.disable.jmx']}")
	private boolean disableJmx;

	@Value("#{cassandraProperties['achilles.cassandra.disable.metrics']}")
	private boolean disableMetrics;

	@Value("#{cassandraProperties['achilles.cassandra.ssl.enabled']}")
	private boolean sslEnabled;

	@Autowired
	private SSLOptions sslOptions;

	@Autowired
	private ObjectMapperFactory objecMapperFactory;

	@Value("#{cassandraProperties['achilles.consistency.read.default']}")
	private String consistencyLevelReadDefault;

	@Value("#{cassandraProperties['achilles.consistency.write.default']}")
	private String consistencyLevelWriteDefault;

	@Value("#{cassandraProperties['achilles.consistency.read.map']}")
	private String consistencyLevelReadMap;

	@Value("#{cassandraProperties['achilles.consistency.write.map']}")
	private String consistencyLevelWriteMap;

	@Value("#{cassandraProperties['achilles.ddl.force.column.family.creation']}")
	private String forceColumnFamilyCreation;

	private CQLEntityManagerFactory emf;

	@PostConstruct
	public void initialize() {
		Map<String, Object> configMap = extractConfigParams();
		emf = new CQLEntityManagerFactory(configMap);
	}

	@Bean
	public CQLEntityManager getEntityManager() {
		return emf.createEntityManager();
	}

	private Map<String, Object> extractConfigParams() {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put(ENTITY_PACKAGES_PARAM, entityPackages);

		configMap.put(CONNECTION_CONTACT_POINTS_PARAM, contactPoints);
		configMap.put(CONNECTION_PORT_PARAM, port);
		configMap.put(KEYSPACE_NAME_PARAM, keyspaceName);

		// Default compression set to Snappy
		configMap.put(COMPRESSION_TYPE, Compression.SNAPPY);

		configMap.put(RETRY_POLICY, retryPolicy);
		configMap.put(LOAD_BALANCING_POLICY, loadBalancingPolicy);
		configMap.put(RECONNECTION_POLICY, reconnectionPolicy);

		if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
			configMap.put(USERNAME, username);
			configMap.put(PASSWORD, password);
		}

		configMap.put(DISABLE_JMX, disableJmx);
		configMap.put(DISABLE_METRICS, disableMetrics);

		configMap.put(SSL_ENABLED, sslEnabled);
		configMap.put(SSL_OPTIONS, sslOptions);

		configMap.put(OBJECT_MAPPER_FACTORY_PARAM, objecMapperFactory);

		if (isNotBlank(consistencyLevelReadDefault)) {
			configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM, consistencyLevelReadDefault);
		}
		if (isNotBlank(consistencyLevelWriteDefault)) {
			configMap.put(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM, consistencyLevelWriteDefault);
		}

		if (isNotBlank(consistencyLevelReadMap)) {
			configMap.put(CONSISTENCY_LEVEL_READ_MAP_PARAM, extractConsistencyMap(consistencyLevelReadMap));
		}
		if (isNotBlank(consistencyLevelWriteMap)) {
			configMap.put(CONSISTENCY_LEVEL_WRITE_MAP_PARAM, extractConsistencyMap(consistencyLevelWriteMap));
		}

		configMap.put(FORCE_CF_CREATION_PARAM, Boolean.parseBoolean(forceColumnFamilyCreation));

		return configMap;
	}

	private Map<String, String> extractConsistencyMap(String consistencyMapProperty) {
		Map<String, String> consistencyMap = new HashMap<String, String>();

		for (String entry : split(consistencyMapProperty, ",")) {
			String[] entryValue = StringUtils.split(entry, ":");
			assert entryValue.length == 2 : "Invalid map value : " + entry + " for the property : "
					+ consistencyMapProperty;
			consistencyMap.put(entryValue[0], entryValue[1]);
		}
		return consistencyMap;
	}
}
