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

import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class ThriftArgumentExtractor extends ArgumentExtractor {

	public Cluster initCluster(Map<String, Object> configurationMap) {

		Cluster cluster = (Cluster) configurationMap.get(CLUSTER_PARAM);
		if (cluster == null) {
			String cassandraHost = (String) configurationMap.get(HOSTNAME_PARAM);
			String cassandraClusterName = (String) configurationMap.get(CLUSTER_NAME_PARAM);

			Validator.validateNotBlank(cassandraHost, "Either '" + CLUSTER_PARAM + "' property or '" + HOSTNAME_PARAM
					+ "'/'" + CLUSTER_NAME_PARAM
					+ "' properties should be provided for Achilles ThrifEntityManagerFactory bootstraping");
			Validator.validateNotBlank(cassandraClusterName, "Either '" + CLUSTER_PARAM + "' property or '"
					+ HOSTNAME_PARAM + "'/'" + CLUSTER_NAME_PARAM
					+ "' properties should be provided for Achilles ThrifEntityManagerFactory bootstraping");

			cluster = HFactory.getOrCreateCluster(cassandraClusterName, new CassandraHostConfigurator(cassandraHost));
		}

		return cluster;
	}

	public Keyspace initKeyspace(Cluster cluster, ThriftConsistencyLevelPolicy consistencyPolicy,
			Map<String, Object> configurationMap) {
		Keyspace keyspace = (Keyspace) configurationMap.get(KEYSPACE_PARAM);
		if (keyspace == null) {
			String keyspaceName = (String) configurationMap.get(KEYSPACE_NAME_PARAM);
			Validator.validateNotBlank(keyspaceName, "Either '" + KEYSPACE_PARAM + "' property or '"
					+ KEYSPACE_NAME_PARAM
					+ "' property should be provided for Achilles ThrifEntityManagerFactory bootstraping");

			keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		}
		keyspace.setConsistencyLevelPolicy(consistencyPolicy);
		return keyspace;
	}
}
