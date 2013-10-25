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
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.CASSANDRA_THRIFT_PORT;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_HOST;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.KEYSPACE_NAME;

import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManager;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManagerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import info.archinnov.achilles.validation.Validator;
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
	private static final Logger LOGGER = LoggerFactory.getLogger(ThriftEmbeddedServer.class);

	private static String entityPackages;
	private static boolean initialized = false;
	private static Cluster cluster;
	private static Keyspace keyspace;
	private static ThriftConsistencyLevelPolicy policy;

	private static ThriftPersistenceManagerFactory pmf;
	private static ThriftPersistenceManager manager;

	public ThriftEmbeddedServer(Map<String,Object> originalParameters) {
        Map<String, Object> parameters = CassandraEmbeddedConfigParameters
                .mergeWithDefaultParameters(originalParameters);
		synchronized (SEMAPHORE) {
			if (!initialized) {
                ThriftEmbeddedServer.entityPackages = (String) parameters.get(ENTITY_PACKAGES);
                Validator.validateNotBlank(entityPackages,"Entity packages should be provided");
                startServer(parameters);
				initialize(parameters);
			}
		}
	}

	private void initialize(Map<String,Object> parameters) {


        Map<String,Object> achillesConfigMap = new HashMap<String, Object>();
        String keyspaceName = (String) parameters.get(KEYSPACE_NAME);
        String hostname;
        int thriftPort;

        String cassandraHost = System.getProperty(CASSANDRA_HOST);
		if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":")) {
			CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator(cassandraHost);
			cluster = HFactory.getOrCreateCluster("achilles", hostConfigurator);
			keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		} else {

            hostname = DEFAULT_CASSANDRA_HOST;
            thriftPort = (Integer)parameters.get(CASSANDRA_THRIFT_PORT);
			createKeyspaceIfNeeded(hostname,thriftPort,keyspaceName);
			cluster = HFactory.getOrCreateCluster("Achilles-cluster", hostname + ":"
					+ thriftPort);
			keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		}


        achillesConfigMap.put(ENTITY_PACKAGES_PARAM, entityPackages);
        achillesConfigMap.put(CLUSTER_PARAM, cluster);
        achillesConfigMap.put(KEYSPACE_PARAM, getKeyspace());
        achillesConfigMap.put(FORCE_CF_CREATION_PARAM, true);

		pmf = new ThriftPersistenceManagerFactory(achillesConfigMap);
		manager = pmf.createPersistenceManager();
		policy = pmf.getConsistencyPolicy();
		initialized = true;
	}

	private void createKeyspaceIfNeeded(String hostname,int thriftPort,String keyspaceName) {

		TTransport tr = new TFramedTransport(new TSocket(hostname, thriftPort));
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

	public ThriftPersistenceManagerFactory getPersistenceManagerFactory() {
		return pmf;
	}

	public ThriftPersistenceManager getPersistenceManager() {
		return manager;
	}

	public ThriftConsistencyLevelPolicy getConsistencyPolicy() {
		return policy;
	}

	public static ThriftConsistencyLevelPolicy policy() {
		return policy;
	}
}
