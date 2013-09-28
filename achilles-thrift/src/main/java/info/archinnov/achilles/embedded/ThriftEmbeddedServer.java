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
package info.archinnov.achilles.embedded;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ThriftConfigurationParameters.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;

import java.util.ArrayList;
import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class ThriftEmbeddedServer extends AchillesEmbeddedServer {

	private static final Object SEMAPHORE = new Object();
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ThriftEmbeddedServer.class);

	private static String entityPackages;
	private static boolean initialized = false;
	private static Cluster cluster;
	private static Keyspace keyspace;
	private static ThriftConsistencyLevelPolicy policy;

	private static ThriftEntityManagerFactory emf;
	private static ThriftEntityManager em;

	public ThriftEmbeddedServer(boolean cleanCassandraDataFile,
			String entityPackages, String keyspaceName) {
		if (StringUtils.isEmpty(entityPackages))
			throw new IllegalArgumentException(
					"Entity packages should be provided");

		synchronized (SEMAPHORE) {
			if (!initialized) {
				startServer(cleanCassandraDataFile);
				ThriftEmbeddedServer.entityPackages = entityPackages;
				initialize(keyspaceName);
			}
		}
	}

	private void initialize(String keyspaceName) {
		String cassandraHost = System.getProperty(CASSANDRA_HOST);
		if (StringUtils.isNotBlank(cassandraHost)
				&& cassandraHost.contains(":")) {
			CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator(
					cassandraHost);
			cluster = HFactory.getOrCreateCluster("achilles", hostConfigurator);
			keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		} else {
			createAchillesKeyspace(keyspaceName);
			cluster = HFactory.getOrCreateCluster("Achilles-cluster",
					CASSANDRA_TEST_HOST + ":" + CASSANDRA_THRIFT_TEST_PORT);
			keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		}

		Map<String, Object> configMap = ImmutableMap.of(ENTITY_PACKAGES_PARAM,
				entityPackages, CLUSTER_PARAM, cluster, KEYSPACE_PARAM,
				getKeyspace(), FORCE_CF_CREATION_PARAM, true);

		emf = new ThriftEntityManagerFactory(configMap);
		em = emf.createEntityManager();
		policy = emf.getConsistencyPolicy();
		initialized = true;
	}

	private void createAchillesKeyspace(String keyspaceName) {

		TTransport tr = new TFramedTransport(new TSocket("localhost",
				CASSANDRA_THRIFT_TEST_PORT));
		TProtocol proto = new TBinaryProtocol(tr, true, true);
		Cassandra.Client client = new Cassandra.Client(proto);
		try {
			tr.open();

			for (KsDef ksDef : client.describe_keyspaces()) {
				if (StringUtils.equals(ksDef.getName(), keyspaceName)) {
					return;
				}
			}
			LOGGER.info("Create keyspace " + keyspaceName);

			KsDef ksDef = new KsDef();
			ksDef.name = keyspaceName;
			ksDef.replication_factor = 1;
			ksDef.strategy_options = ImmutableMap.of("replication_factor", "1");
			ksDef.strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
			ksDef.setCf_defs(new ArrayList<CfDef>());

			client.system_add_keyspace(ksDef);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tr.close();
		}
	}

	public Cluster getCluster() {
		return cluster;
	}

	public Keyspace getKeyspace() {
		return keyspace;
	}

	public ThriftEntityManagerFactory getEmf() {
		return emf;
	}

	public ThriftEntityManager getEm() {
		return em;
	}

	public ThriftConsistencyLevelPolicy getConsistencyPolicy() {
		return policy;
	}

	public static ThriftConsistencyLevelPolicy policy() {
		return policy;
	}
}
