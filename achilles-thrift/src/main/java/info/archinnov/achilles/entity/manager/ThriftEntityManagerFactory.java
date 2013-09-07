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
package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.configuration.ThriftArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext.Impl;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftDaoContextBuilder;
import info.archinnov.achilles.context.ThriftPersistenceContextFactory;
import info.archinnov.achilles.table.ThriftTableCreator;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Collections;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftEntityManagerFactory extends EntityManagerFactory {

	private static final Logger log = LoggerFactory
			.getLogger(ThriftEntityManagerFactory.class);

	private Cluster cluster;
	private Keyspace keyspace;

	private ThriftDaoContext daoContext;
	private ThriftPersistenceContextFactory contextFactory;
	private ThriftConsistencyLevelPolicy policy;

	/**
	 * Create a new ThriftEntityManagerFactoryImpl with a configuration map
	 * 
	 * @param configurationMap
	 *            Check documentation for more details on configuration
	 *            parameters
	 */
	public ThriftEntityManagerFactory(Map<String, Object> configurationMap) {
		super(configurationMap, new ThriftArgumentExtractor());
		configContext.setImpl(Impl.THRIFT);

		ThriftArgumentExtractor thriftArgumentExtractor = new ThriftArgumentExtractor();
		cluster = thriftArgumentExtractor.initCluster(configurationMap);
		keyspace = thriftArgumentExtractor.initKeyspace(cluster,
				(ThriftConsistencyLevelPolicy) configContext
						.getConsistencyPolicy(), configurationMap);

		log.info(
				"Initializing Achilles ThriftEntityManagerFactory for cluster '{}' and keyspace '{}' ",
				cluster.getName(), keyspace.getKeyspaceName());

		boolean hasSimpleCounter = bootstrap();
		new ThriftTableCreator(cluster, keyspace).validateOrCreateTables(
				entityMetaMap, configContext, hasSimpleCounter);
		daoContext = new ThriftDaoContextBuilder().buildDao(cluster, keyspace,
				entityMetaMap, configContext, hasSimpleCounter);
		contextFactory = new ThriftPersistenceContextFactory(daoContext,
				configContext, entityMetaMap);

	}

	/**
	 * Create a new ThriftEntityManager. This instance of ThriftEntityManager is
	 * <strong>thread-safe</strong>
	 * 
	 * @return ThriftEntityManager
	 */
	public ThriftEntityManager createEntityManager() {
		log.info("Create new Thrift-based Entity Manager ");

		return new ThriftEntityManager(
				Collections.unmodifiableMap(entityMetaMap), //
				contextFactory, daoContext, configContext);
	}

	/**
	 * Create a new state-full EntityManager for batch handling <br/>
	 * <br/>
	 * 
	 * <strong>WARNING : This EntityManager is state-full and not thread-safe.
	 * In case of exception, you MUST not re-use it but create another
	 * one</strong>
	 * 
	 * @return a new state-full EntityManager
	 */
	public ThriftBatchingEntityManager createBatchingEntityManager() {
		return new ThriftBatchingEntityManager(entityMetaMap, contextFactory,
				daoContext, configContext);
	}

	@Override
	protected AchillesConsistencyLevelPolicy initConsistencyLevelPolicy(
			Map<String, Object> configurationMap,
			ArgumentExtractor argumentExtractor) {
		log.info("Initializing new Achilles Configurable Consistency Level Policy from arguments ");

		ConsistencyLevel defaultReadConsistencyLevel = argumentExtractor
				.initDefaultReadConsistencyLevel(configurationMap);
		ConsistencyLevel defaultWriteConsistencyLevel = argumentExtractor
				.initDefaultWriteConsistencyLevel(configurationMap);
		Map<String, ConsistencyLevel> readConsistencyMap = argumentExtractor
				.initReadConsistencyMap(configurationMap);
		Map<String, ConsistencyLevel> writeConsistencyMap = argumentExtractor
				.initWriteConsistencyMap(configurationMap);

		policy = new ThriftConsistencyLevelPolicy(defaultReadConsistencyLevel,
				defaultWriteConsistencyLevel, readConsistencyMap,
				writeConsistencyMap);
		return policy;
	}

	protected void setThriftDaoContext(ThriftDaoContext thriftDaoContext) {
		this.daoContext = thriftDaoContext;
	}

	public ThriftConsistencyLevelPolicy getConsistencyPolicy() {
		return policy;
	}
}
