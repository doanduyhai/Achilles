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
package info.archinnov.achilles.table;

import static info.archinnov.achilles.counter.AchillesCounter.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.IndexProperties;
import info.archinnov.achilles.entity.metadata.InternalTimeUUID;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.validation.Validator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public class CQLTableCreator {
	private static final Logger log = LoggerFactory.getLogger(CQLTableCreator.class);

	public static final String TABLE_PATTERN = "[a-zA-Z0-9_]+";
	static final String ACHILLES_DDL_SCRIPT = "ACHILLES_DDL_SCRIPT";

	public Map<String, TableMetadata> fetchTableMetaData(KeyspaceMetadata keyspaceMeta, String keyspaceName) {
		Map<String, TableMetadata> tableMetas = new HashMap<String, TableMetadata>();

		Validator.validateTableTrue(keyspaceMeta != null, "Keyspace '%s' doest not exist or cannot be found",
				keyspaceName);

		for (TableMetadata tableMeta : keyspaceMeta.getTables()) {
			tableMetas.put(tableMeta.getName(), tableMeta);
		}
		return tableMetas;
	}

	public void createTableForEntity(Session session, EntityMeta entityMeta, boolean forceColumnFamilyCreation) {
		String tableName = entityMeta.getTableName().toLowerCase();
		if (forceColumnFamilyCreation) {
			log.debug("Force creation of table for entityMeta {}", entityMeta.getClassName());
			createTableForEntity(session, entityMeta);
		} else {
			throw new AchillesInvalidTableException("The required table '" + tableName
					+ "' does not exist for entity '" + entityMeta.getClassName() + "'");
		}
	}

	public void createTableForCounter(Session session, boolean forceColumnFamilyCreation) {
		if (forceColumnFamilyCreation) {
			CQLTableBuilder builder = CQLTableBuilder.createTable(CQL_COUNTER_TABLE);
			builder.addColumn(CQL_COUNTER_FQCN, String.class);
			builder.addColumn(CQL_COUNTER_PRIMARY_KEY, String.class);
			builder.addColumn(CQL_COUNTER_PROPERTY_NAME, String.class);
			builder.addColumn(CQL_COUNTER_VALUE, Counter.class);
			builder.addPartitionComponent(CQL_COUNTER_FQCN);
			builder.addPartitionComponent(CQL_COUNTER_PRIMARY_KEY);
			builder.addClusteringComponent(CQL_COUNTER_PROPERTY_NAME);

			builder.addComment("Create default Achilles counter table '" + CQL_COUNTER_TABLE + "'");

			session.execute(builder.generateDDLScript());
		} else {
			throw new AchillesInvalidTableException("The required generic table '" + CQL_COUNTER_TABLE
					+ "' does not exist");
		}
	}

	private void createTableForEntity(Session session, EntityMeta entityMeta) {
		log.debug("Creating table for entityMeta {}", entityMeta.getClassName());
		if (entityMeta.isClusteredCounter()) {
			createTableForClusteredCounter(session, entityMeta);
		} else {
			createTable(session, entityMeta);
		}
	}

	private void createTable(Session session, EntityMeta entityMeta) {
		String tableName = entityMeta.getTableName();
		CQLTableBuilder builder = CQLTableBuilder.createTable(tableName);
		for (PropertyMeta pm : entityMeta.getAllMetasExceptIdMeta()) {
			String propertyName = pm.getPropertyName();
			Class<?> keyClass = pm.getKeyClass();
			Class<?> valueClass = pm.getValueClassForTableCreation();
			switch (pm.type()) {
			case SIMPLE:
			case LAZY_SIMPLE:
				builder.addColumn(propertyName, valueClass);
				if (pm.isIndexed()) {
					builder.addIndex(new IndexProperties(pm.getIndexProperties().getName(), propertyName));
				}
				break;
			case LIST:
			case LAZY_LIST:
				builder.addList(propertyName, valueClass);
				break;
			case SET:
			case LAZY_SET:
				builder.addSet(propertyName, valueClass);
				break;
			case MAP:
			case LAZY_MAP:
				builder.addMap(propertyName, keyClass, pm.getValueClass());
				break;
			default:
				break;
			}

		}
		buildPrimaryKey(entityMeta.getIdMeta(), builder);
		builder.addComment("Create table for entity '" + entityMeta.getClassName() + "'");
		session.execute(builder.generateDDLScript());
		if (builder.hasIndices()) {
			for (String indexScript : builder.generateIndices()) {
				session.execute(indexScript);
			}
		}

	}

	private void createTableForClusteredCounter(Session session, EntityMeta meta) {
		PropertyMeta pm = meta.getFirstMeta();

		log.debug("Creating table for counter property {} for entity {}", pm.getPropertyName(), meta.getClassName());

		CQLTableBuilder builder = CQLTableBuilder.createCounterTable(meta.getTableName());
		PropertyMeta idMeta = meta.getIdMeta();
		buildPrimaryKey(idMeta, builder);
		builder.addColumn(pm.getPropertyName(), pm.getValueClass());

		builder.addComment("Create table for clustered counter entity '" + meta.getClassName() + "'");

		session.execute(builder.generateDDLScript());

	}

	private void buildPrimaryKey(PropertyMeta pm, CQLTableBuilder builder) {
		if (pm.isEmbeddedId()) {
			addPrimaryKeyComponents(pm, builder, true);
			addPrimaryKeyComponents(pm, builder, false);
		} else {
			String columnName = pm.getPropertyName();
			builder.addColumn(columnName, pm.getValueClassForTableCreation());
			builder.addPartitionComponent(columnName);
		}
	}

	private void addPrimaryKeyComponents(PropertyMeta pm, CQLTableBuilder builder, boolean partitionKey) {
		List<String> componentNames;
		List<Class<?>> componentClasses;

		if (partitionKey) {
			componentNames = pm.getPartitionComponentNames();
			componentClasses = pm.getPartitionComponentClasses();
		} else {
			componentNames = pm.getClusteringComponentNames();
			componentClasses = pm.getClusteringComponentClasses();
			builder.setReversedClusteredComponent(pm.getReversedComponent());
		}
		for (int i = 0; i < componentNames.size(); i++) {
			String componentName = componentNames.get(i);
			Class<?> javaType = componentClasses.get(i);
			if (pm.isComponentTimeUUID(componentName)) {
				javaType = InternalTimeUUID.class;
			}

			builder.addColumn(componentName, javaType);
			if (partitionKey)
				builder.addPartitionComponent(componentName);
			else
				builder.addClusteringComponent(componentName);
		}
	}
}
