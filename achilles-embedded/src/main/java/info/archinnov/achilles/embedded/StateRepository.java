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

import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Session;

public enum StateRepository {
	REPOSITORY;

	private static final Map<String, Boolean> KEYSPACE_BOOTSTRAP_MAP = new HashMap<>();

	private static final Map<String, Session> SESSIONS_MAP = new HashMap<>();

	private static final Map<String, PersistenceManagerFactory> FACTORIES_MAP = new HashMap<>();

	private static final Map<String, PersistenceManager> MANAGERS_MAP = new HashMap<>();

	public boolean keyspaceAlreadyBootstrapped(String keyspaceName) {
		return KEYSPACE_BOOTSTRAP_MAP.containsKey(keyspaceName);
	}

	public void markKeyspaceAsBootstrapped(String keyspaceName) {
		KEYSPACE_BOOTSTRAP_MAP.put(keyspaceName, true);
	}

	public void addNewSessionToKeyspace(String keyspaceName, Session nativeSession) {
		SESSIONS_MAP.put(keyspaceName, nativeSession);
	}

	public Session getSessionForKeyspace(String keyspaceName) {
		if (!SESSIONS_MAP.containsKey(keyspaceName)) {
			throw new IllegalStateException(String.format("Cannot find Session for keyspace '%s'", keyspaceName));
		}
		return SESSIONS_MAP.get(keyspaceName);
	}

	public void addNewManagerFactoryToKeyspace(String keyspaceName, PersistenceManagerFactory factory) {
		FACTORIES_MAP.put(keyspaceName, factory);
	}

	public PersistenceManagerFactory getManagerFactoryForKeyspace(String keyspaceName) {
		if (!FACTORIES_MAP.containsKey(keyspaceName)) {
			throw new IllegalStateException(String.format("Cannot find PersistenceManagerFactory for keyspace '%s'",
					keyspaceName));
		}
		return FACTORIES_MAP.get(keyspaceName);
	}

	public void addNewManagerToKeyspace(String keyspaceName, PersistenceManager manager) {
		MANAGERS_MAP.put(keyspaceName, manager);
	}

	public PersistenceManager getManagerForKeyspace(String keyspaceName) {
		if (!MANAGERS_MAP.containsKey(keyspaceName)) {
			throw new IllegalStateException(String.format("Cannot find PersistenceManager for keyspace '%s'",
					keyspaceName));
		}
		return MANAGERS_MAP.get(keyspaceName);
	}
}
