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

package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.parsing.EntityExplorer;
import info.archinnov.achilles.table.CQLTableCreator;
import info.archinnov.achilles.table.CQLTableValidator;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public class SchemaContext {

	private static final Logger log = LoggerFactory.getLogger(EntityExplorer.class);
	private boolean forceColumnFamilyCreation;
	private Session session;
	private String keyspaceName;
	private Cluster cluster;
	private Map<Class<?>, EntityMeta> entityMetaMap;
	private boolean hasCounter;
	private CQLTableCreator tableCreator = new CQLTableCreator();
	private CQLTableValidator tableValidator = new CQLTableValidator();

	public SchemaContext(boolean forceColumnFamilyCreation, Session session, String keyspaceName, Cluster cluster,
			Map<Class<?>, EntityMeta> entityMetaMap, boolean hasCounter) {
		this.forceColumnFamilyCreation = forceColumnFamilyCreation;
		this.session = session;
		this.keyspaceName = keyspaceName;
		this.cluster = cluster;
		this.entityMetaMap = entityMetaMap;
		this.hasCounter = hasCounter;
	}

	public Session getSession() {
		return session;
	}

	public boolean hasSimpleCounter() {
		return hasCounter;
	}

	public Set<Entry<Class<?>, EntityMeta>> entityMetaEntrySet() {
		return entityMetaMap.entrySet();
	}

	public void validateForEntity(EntityMeta entityMeta, TableMetadata tableMetaData) {
		tableValidator.validateForEntity(entityMeta, tableMetaData);
	}

	public void validateAchillesCounter() {
		tableValidator.validateAchillesCounter(cluster.getMetadata().getKeyspace(keyspaceName), keyspaceName);
	}

	public Map<String, TableMetadata> fetchTableMetaData() {
		return tableCreator.fetchTableMetaData(cluster.getMetadata().getKeyspace(keyspaceName), keyspaceName);
	}

	public void createTableForEntity(EntityMeta entityMeta) {
		tableCreator.createTableForEntity(session, entityMeta, forceColumnFamilyCreation);
	}

	public void createTableForCounter() {
		tableCreator.validateOrCreateTableForCounter(session, forceColumnFamilyCreation);
	}
}
