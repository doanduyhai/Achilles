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

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.*;
import static org.apache.commons.lang.StringUtils.*;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManager;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManagerFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ThriftPersistenceManagerJavaConfigSample {

	@Value("#{cassandraProperties['achilles.entity.packages']}")
	private String entityPackages;

	@Autowired(required = true)
	private Cluster cluster;

	@Autowired(required = true)
	private Keyspace keyspace;

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

	private ThriftPersistenceManagerFactory pmf;

	@PostConstruct
	public void initialize() {
		Map<String, Object> configMap = extractConfigParams();
		pmf = new ThriftPersistenceManagerFactory(configMap);
	}

	@Bean
	public ThriftPersistenceManager getPersistenceManager() {
		return pmf.createPersistenceManager();
	}

	private Map<String, Object> extractConfigParams() {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put(ENTITY_PACKAGES_PARAM, entityPackages);

		configMap.put(CLUSTER_PARAM, cluster);
		configMap.put(KEYSPACE_NAME_PARAM, keyspace);

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
