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
package info.archinnov.achilles.test.integration;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.*;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.embedded.CassandraEmbeddedServer;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.type.TypedMap;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Session;

public class AchillesInternalCQLResource extends AchillesTestResource {

	private static final String CLEAN_DATA_FILES_PROPERTY = "clean.data.files";

	private static final String ACHILLES_ENTITY_PACKAGES = "info.archinnov.achilles.test.integration.entity";

	private final PersistenceManagerFactory pmf;

	private final PersistenceManager manager;

	private final CassandraEmbeddedServer server;

	private final Session session;

	private boolean cleanDataFiles = true;

	/**
	 * Initialize a new embedded Cassandra server
	 * 
	 * @param tables
	 *            list of tables to truncate before and after tests
	 */
	public AchillesInternalCQLResource(String... tables) {
		super(tables);
		final TypedMap config = buildConfigMap();

		server = new CassandraEmbeddedServer(config);
		pmf = server.getPersistenceManagerFactory(DEFAULT_ACHILLES_TEST_KEYSPACE_NAME);
		manager = server.getPersistenceManager(DEFAULT_ACHILLES_TEST_KEYSPACE_NAME);
		session = server.getNativeSession(DEFAULT_ACHILLES_TEST_KEYSPACE_NAME);
	}

	/**
	 * Initialize a new embedded Cassandra server
	 * 
	 * @param cleanUpSteps
	 *            when to truncate tables for clean up. Possible values are :
	 *            Steps.BEFORE_TEST, Steps.AFTER_TEST and Steps.BOTH (Default
	 *            value) <br/>
	 * <br/>
	 * @param tables
	 *            list of tables to truncate before, after or before and after
	 *            tests, depending on the 'cleanUpSteps' parameters
	 */
	public AchillesInternalCQLResource(Steps cleanUpSteps, String... tables) {
		super(cleanUpSteps, tables);
		final TypedMap config = buildConfigMap();

		server = new CassandraEmbeddedServer(config);
		pmf = server.getPersistenceManagerFactory(DEFAULT_ACHILLES_TEST_KEYSPACE_NAME);
		manager = server.getPersistenceManager(DEFAULT_ACHILLES_TEST_KEYSPACE_NAME);
		session = server.getNativeSession(DEFAULT_ACHILLES_TEST_KEYSPACE_NAME);
	}

	/**
	 * Return a singleton PersistenceManagerFactory
	 * 
	 * @return PersistenceManagerFactory singleton
	 */
	public PersistenceManagerFactory getPersistenceManagerFactory() {
		return pmf;
	}

	/**
	 * Return a singleton PersistenceManager
	 * 
	 * @return PersistenceManager singleton
	 */
	public PersistenceManager getPersistenceManager() {
		return manager;
	}

	/**
	 * Return a native CQL3 Session
	 * 
	 * @return native CQL3 Session
	 */
	public Session getNativeSession() {
		return session;
	}

	@Override
	protected void truncateTables() {
		if (tables != null) {
			for (String table : tables) {
				server.truncateTable(DEFAULT_ACHILLES_TEST_KEYSPACE_NAME, table);
			}
		}
	}

	private TypedMap buildConfigMap() {
		setCleanDataFiles();
		final TypedMap config = new TypedMap();
		config.put(CLEAN_CASSANDRA_DATA_FILES, cleanDataFiles);
		config.put(ENTITY_PACKAGES_PARAM, ACHILLES_ENTITY_PACKAGES);
		config.put(KEYSPACE_NAME_PARAM, DEFAULT_ACHILLES_TEST_KEYSPACE_NAME);
		config.put(KEYSPACE_DURABLE_WRITE, false);
		config.put(ConfigurationParameters.BEAN_VALIDATION_ENABLE, true);
		return config;
	}

	private void setCleanDataFiles() {
		final String cleanDataFiles = System.getProperty(CLEAN_DATA_FILES_PROPERTY);
		if (StringUtils.isNotBlank(cleanDataFiles)) {
			this.cleanDataFiles = Boolean.parseBoolean(cleanDataFiles);
		}
	}
}
