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
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLDaoContextBuilder;
import info.archinnov.achilles.context.CQLPersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.parsing.EntityExplorer;
import info.archinnov.achilles.entity.parsing.EntityParser;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.table.CQLTableCreator;
import info.archinnov.achilles.validation.Validator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CQLPersistenceManagerFactory {
	private static final Logger log = LoggerFactory.getLogger(CQLPersistenceManagerFactory.class);

	private Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
	private ConfigurationContext configContext;
	private List<String> entityPackages;

	protected ArgumentExtractor argumentExtractor = new ArgumentExtractor();
	private EntityParser entityParser = new EntityParser();
	private EntityExplorer entityExplorer = new EntityExplorer();

	private Cluster cluster;
	private Session session;
	private CQLDaoContext daoContext;
	private CQLPersistenceContextFactory contextFactory;

	/**
	 * Create a new CQLPersistenceManagerFactory with a configuration map
	 * 
	 * @param configurationMap
	 *            Check documentation for more details on configuration
	 *            parameters
	 */
	public CQLPersistenceManagerFactory(Map<String, Object> configurationMap) {
		Validator.validateNotNull(configurationMap,
				"Configuration map for PersistenceManagerFactory should not be null");
		Validator.validateNotEmpty(configurationMap,
				"Configuration map for PersistenceManagerFactory should not be empty");

		entityPackages = argumentExtractor.initEntityPackages(configurationMap);
		configContext = parseConfiguration(configurationMap);

		cluster = argumentExtractor.initCluster(configurationMap);
		session = argumentExtractor.initSession(cluster, configurationMap);

		boolean hasSimpleCounter = false;
		if (StringUtils.isNotBlank((String) configurationMap.get(ENTITY_PACKAGES_PARAM))) {
			hasSimpleCounter = bootstrap();
		}

		new CQLTableCreator(cluster, session, (String) configurationMap.get(KEYSPACE_NAME_PARAM))
				.validateOrCreateTables(entityMetaMap, configContext, hasSimpleCounter);

		daoContext = CQLDaoContextBuilder.builder(session).build(entityMetaMap, hasSimpleCounter);
		contextFactory = new CQLPersistenceContextFactory(daoContext, configContext, entityMetaMap);
		registerShutdownHook(cluster);
	}

	/**
	 * Create a new CQLPersistenceManager. This instance of
	 * CQLPersistenceManager is <strong>thread-safe</strong>
	 * 
	 * @return CQLPersistenceManager
	 */
	public CQLPersistenceManager createPersistenceManager() {
		return new CQLPersistenceManager(entityMetaMap, contextFactory, daoContext, configContext);
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
	public CQLBatchingPersistenceManager createBatchingPersistenceManager() {
		return new CQLBatchingPersistenceManager(entityMetaMap, contextFactory, daoContext, configContext);
	}

	protected boolean bootstrap() {
		log.info("Bootstraping Achilles PersistenceManagerFactory ");

		boolean hasSimpleCounter = false;
		try {
			hasSimpleCounter = discoverEntities();
		} catch (Exception e) {
			throw new AchillesException("Exception during entity parsing : " + e.getMessage(), e);
		}

		return hasSimpleCounter;
	}

	protected boolean discoverEntities() throws ClassNotFoundException, IOException {
		log.info("Start discovery of entities, searching in packages '{}'", StringUtils.join(entityPackages, ","));

		List<Class<?>> entities = entityExplorer.discoverEntities(entityPackages);
		boolean hasSimpleCounter = false;
		for (Class<?> entityClass : entities) {
			EntityParsingContext context = new EntityParsingContext(configContext, entityClass);

			EntityMeta entityMeta = entityParser.parseEntity(context);
			entityMetaMap.put(entityClass, entityMeta);
			hasSimpleCounter = context.getHasSimpleCounter() || hasSimpleCounter;
		}

		return hasSimpleCounter;
	}

	protected ConfigurationContext parseConfiguration(Map<String, Object> configurationMap) {
		ConfigurationContext configContext = new ConfigurationContext();
		configContext.setForceColumnFamilyCreation(argumentExtractor.initForceCFCreation(configurationMap));
		configContext.setObjectMapperFactory(argumentExtractor.initObjectMapperFactory(configurationMap));
		configContext.setDefaultReadConsistencyLevel(argumentExtractor
				.initDefaultReadConsistencyLevel(configurationMap));
		configContext.setDefaultWriteConsistencyLevel(argumentExtractor
				.initDefaultWriteConsistencyLevel(configurationMap));
		return configContext;
	}

	protected void setEntityPackages(List<String> entityPackages) {
		this.entityPackages = entityPackages;
	}

	protected void setEntityParser(EntityParser achillesEntityParser) {
		this.entityParser = achillesEntityParser;
	}

	protected void setEntityExplorer(EntityExplorer achillesEntityExplorer) {
		this.entityExplorer = achillesEntityExplorer;
	}

	protected void setEntityMetaMap(Map<Class<?>, EntityMeta> entityMetaMap) {
		this.entityMetaMap = entityMetaMap;
	}

	protected void setConfigContext(ConfigurationContext configContext) {
		this.configContext = configContext;
	}

	private void registerShutdownHook(final Cluster cluster) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.shutdown();
			}
		});
	}
}
