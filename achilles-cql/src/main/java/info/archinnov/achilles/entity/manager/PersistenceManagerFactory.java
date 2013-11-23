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

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.context.PersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.SchemaContext;
import info.archinnov.achilles.entity.discovery.AchillesBootstraper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.validation.Validator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class PersistenceManagerFactory {
	private static final Logger log = LoggerFactory.getLogger(PersistenceManagerFactory.class);

	Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
	ConfigurationContext configContext;
	DaoContext daoContext;
	PersistenceContextFactory contextFactory;
	Map<String, Object> configurationMap;

	private ArgumentExtractor argumentExtractor = new ArgumentExtractor();
	private AchillesBootstraper boostraper = new AchillesBootstraper();

	/**
	 * Constructor for test
	 */
	PersistenceManagerFactory() {
	}

	/**
	 * Create a new PersistenceManagerFactory with a configuration map
	 * 
	 * @param configurationMap
	 *            Check documentation for more details on configuration
	 *            parameters
	 */
	public PersistenceManagerFactory(Map<String, Object> configurationMap) {
		Validator.validateNotNull(configurationMap,
				"Configuration map for PersistenceManagerFactory should not be null");
		Validator.validateNotEmpty(configurationMap,
				"Configuration map for PersistenceManagerFactory should not be empty");
		this.configurationMap = configurationMap;
		bootstrap();
	}

	void bootstrap() {
		List<String> entityPackages = argumentExtractor.initEntityPackages(configurationMap);
		configContext = argumentExtractor.initConfigContext(configurationMap);
		Cluster cluster = argumentExtractor.initCluster(configurationMap);
		Session session = argumentExtractor.initSession(cluster, configurationMap);

		List<Class<?>> candidateClasses = boostraper.discoverEntities(entityPackages);

		boolean hasSimpleCounter = false;
		if (StringUtils.isNotBlank((String) configurationMap.get(ENTITY_PACKAGES_PARAM))) {
			Pair<Map<Class<?>, EntityMeta>, Boolean> pair = boostraper.buildMetaDatas(configContext, candidateClasses);
			entityMetaMap = pair.left;
			hasSimpleCounter = pair.right;
		}

		SchemaContext schemaContext = new SchemaContext(configContext.isForceColumnFamilyCreation(), session,
				(String) configurationMap.get(KEYSPACE_NAME_PARAM), cluster, entityMetaMap, hasSimpleCounter);
		boostraper.validateOrCreateTables(schemaContext);

		daoContext = boostraper.buildDaoContext(session, entityMetaMap, hasSimpleCounter);
		contextFactory = new PersistenceContextFactory(daoContext, configContext, entityMetaMap);
		registerShutdownHook(cluster);
	}

	/**
	 * Create a new PersistenceManager. This instance of
	 * PersistenceManager is <strong>thread-safe</strong>
	 * 
	 * @return PersistenceManager
	 */
	public PersistenceManager createPersistenceManager() {
		return new PersistenceManager(entityMetaMap, contextFactory, daoContext, configContext);
	}

	/**
	 * Create a new state-full PersistenceManager for batch handling <br/>
	 * <br/>
	 * 
	 * <strong>WARNING : This PersistenceManager is state-full and not
	 * thread-safe. In case of exception, you MUST not re-use it but create
	 * another one</strong>
	 * 
	 * @return a new state-full PersistenceManager
	 */
	public BatchingPersistenceManager createBatchingPersistenceManager() {
		return new BatchingPersistenceManager(entityMetaMap, contextFactory, daoContext, configContext);
	}

	void registerShutdownHook(final Cluster cluster) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.shutdown();
			}
		});
	}
}
