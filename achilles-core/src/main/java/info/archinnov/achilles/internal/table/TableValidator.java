/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.table;

import static com.datastax.driver.core.DataType.*;
import static info.archinnov.achilles.counter.AchillesCounter.*;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLType;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.InternalTimeUUID;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;

import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

public class TableValidator {

	private static final Logger log = LoggerFactory.getLogger(TableValidator.class);

	private ColumnMetaDataComparator columnMetaDataComparator = new ColumnMetaDataComparator();

	public void validateForEntity(EntityMeta entityMeta, TableMetadata tableMetadata) {
		log.debug("Validate existing table {} for {}", tableMetadata.getName(), entityMeta);
		PropertyMeta idMeta = entityMeta.getIdMeta();
		if (entityMeta.isClusteredCounter()) {

		}
		validateTable(entityMeta, tableMetadata, idMeta);

	}

	private void validateTable(EntityMeta entityMeta, TableMetadata tableMetadata, PropertyMeta idMeta) {
		if (idMeta.isEmbeddedId()) {
			validatePrimaryKeyComponents(tableMetadata, idMeta, true);
			validatePrimaryKeyComponents(tableMetadata, idMeta, false);
		} else {
			validateColumn(tableMetadata, idMeta.getPropertyName().toLowerCase(),
					idMeta.getValueClassForTableCreation(), idMeta.isIndexed());
		}

		for (PropertyMeta pm : entityMeta.getAllMetasExceptIdAndCounters()) {
			switch (pm.type()) {
			case SIMPLE:
				validateColumn(tableMetadata, pm.getPropertyName().toLowerCase(), pm.getValueClassForTableCreation(),
						pm.isIndexed());
				break;
			case LIST:
			case SET:
			case MAP:
				validateCollectionAndMapColumn(tableMetadata, pm);
				break;
			default:
				break;
			}
		}
	}

	public void validateAchillesCounter(KeyspaceMetadata keyspaceMetaData, String keyspaceName) {
		log.debug("Validate existing Achilles Counter table");
		Name textTypeName = text().getName();
		Name counterTypeName = counter().getName();

		TableMetadata tableMetaData = keyspaceMetaData.getTable(CQL_COUNTER_TABLE);
		Validator.validateTableTrue(tableMetaData != null, "Cannot find table '%s' from keyspace '%s'",
				CQL_COUNTER_TABLE, keyspaceName);

		ColumnMetadata fqcnColumn = tableMetaData.getColumn(CQL_COUNTER_FQCN);
		Validator.validateTableTrue(fqcnColumn != null, "Cannot find column '%s' from table '%s'", CQL_COUNTER_FQCN,
				CQL_COUNTER_TABLE);
		Validator.validateTableTrue(fqcnColumn.getType().getName() == textTypeName,
				"Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_FQCN, fqcnColumn.getType().getName(),
				textTypeName);
		Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getPartitionKey(), fqcnColumn),
				"Column '%s' of table '%s' should be a partition key component", CQL_COUNTER_FQCN, CQL_COUNTER_TABLE);

		ColumnMetadata pkColumn = tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY);
		Validator.validateTableTrue(pkColumn != null, "Cannot find column '%s' from table '%s'",
				CQL_COUNTER_PRIMARY_KEY, CQL_COUNTER_TABLE);
		Validator.validateTableTrue(pkColumn.getType().getName() == textTypeName,
				"Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_PRIMARY_KEY, pkColumn.getType()
						.getName(), textTypeName);
		Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getPartitionKey(), pkColumn),
				"Column '%s' of table '%s' should be a partition key component", CQL_COUNTER_PRIMARY_KEY,
				CQL_COUNTER_TABLE);

		ColumnMetadata propertyNameColumn = tableMetaData.getColumn(CQL_COUNTER_PROPERTY_NAME);
		Validator.validateTableTrue(propertyNameColumn != null, "Cannot find column '%s' from table '%s'",
				CQL_COUNTER_PROPERTY_NAME, CQL_COUNTER_TABLE);
		Validator.validateTableTrue(propertyNameColumn.getType().getName() == textTypeName,
				"Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_PROPERTY_NAME, propertyNameColumn
						.getType().getName(), textTypeName);
		Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getClusteringColumns(), propertyNameColumn),
				"Column '%s' of table '%s' should be a clustering key component", CQL_COUNTER_PROPERTY_NAME,
				CQL_COUNTER_TABLE);

		ColumnMetadata counterValueColumn = tableMetaData.getColumn(CQL_COUNTER_VALUE);
		Validator.validateTableTrue(counterValueColumn != null, "Cannot find column '%s' from table '%s'",
				CQL_COUNTER_VALUE, CQL_COUNTER_TABLE);
		Validator.validateTableTrue(counterValueColumn.getType().getName() == counterTypeName,
				"Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_VALUE, counterValueColumn.getType()
						.getName(), counterTypeName);
	}

	private void validateColumn(TableMetadata tableMetaData, String columnName, Class<?> columnJavaType, boolean indexed) {

		log.debug("Validate existing column {} from table {} against type {}", columnName, tableMetaData.getName(),
				columnJavaType);

		String tableName = tableMetaData.getName();
		ColumnMetadata columnMetadata = tableMetaData.getColumn(columnName);
		Name expectedType = toCQLType(columnJavaType);

		Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", columnName,
				tableName);

		boolean columnIsIndexed = columnMetadata.getIndex() != null;

		Validator.validateTableFalse((columnIsIndexed ^ indexed),
				"Column '%s' in the table '%s' is indexed (or not) whereas metadata indicates it" + " is (or not)",
				columnName, tableName);
		Name realType = columnMetadata.getType().getName();

		/*
		 * See JIRA
		 */
		if (realType == Name.CUSTOM) {
			realType = Name.BLOB;
		}

		Validator.validateTableTrue(expectedType == realType,
				"Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName,
				realType, expectedType);
	}

	private void validatePartitionComponent(TableMetadata tableMetaData, String columnName, Class<?> columnJavaType) {

		log.debug("Validate existing partition key component {} from table {} against type {}", columnName,
				tableMetaData.getName(), columnJavaType);

		validateColumn(tableMetaData, columnName, columnJavaType, false);
		ColumnMetadata columnMetadata = tableMetaData.getColumn(columnName);

		Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getPartitionKey(), columnMetadata),
				"Column '%s' of table '%s' should be a partition key component", columnName, tableMetaData.getName());
	}

	private void validateClusteringComponent(TableMetadata tableMetaData, String columnName, Class<?> columnJavaType) {

		log.debug("Validate existing clustering column {} from table {} against type {}", columnName,
				tableMetaData.getName(), columnJavaType);

		validateColumn(tableMetaData, columnName, columnJavaType, false);
		ColumnMetadata columnMetadata = tableMetaData.getColumn(columnName);
		Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getClusteringColumns(), columnMetadata),
				"Column '%s' of table '%s' should be a clustering key component", columnName, tableMetaData.getName());
	}

	private void validateCollectionAndMapColumn(TableMetadata tableMetadata, PropertyMeta pm) {

		log.debug("Validate existing collection/map column {} from table {}");

		String columnName = pm.getPropertyName().toLowerCase();
		String tableName = tableMetadata.getName();
		ColumnMetadata columnMetadata = tableMetadata.getColumn(columnName);

		Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", columnName,
				tableName);
		Name realType = columnMetadata.getType().getName();
		Name expectedValueType = toCQLType(pm.getValueClassForTableCreation());

		switch (pm.type()) {
		case LIST:
			Validator.validateTableTrue(realType == Name.LIST,
					"Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName,
					realType, Name.LIST);
			Name realListValueType = columnMetadata.getType().getTypeArguments().get(0).getName();
			Validator.validateTableTrue(realListValueType == expectedValueType,
					"Column '%s' of table '%s' of type 'List<%s>' should be of type 'List<%s>' indeed", columnName,
					tableName, realListValueType, expectedValueType);

			break;
		case SET:
			Validator.validateTableTrue(realType == Name.SET,
					"Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName,
					realType, Name.SET);
			Name realSetValueType = columnMetadata.getType().getTypeArguments().get(0).getName();

			Validator.validateTableTrue(realSetValueType == expectedValueType,
					"Column '%s' of table '%s' of type 'Set<%s>' should be of type 'Set<%s>' indeed", columnName,
					tableName, realSetValueType, expectedValueType);
			break;
		case MAP:
			Validator.validateTableTrue(realType == Name.MAP,
					"Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName,
					realType, Name.MAP);

			Name expectedMapKeyType = toCQLType(pm.getKeyClass());
			Name realMapKeyType = columnMetadata.getType().getTypeArguments().get(0).getName();
			Name realMapValueType = columnMetadata.getType().getTypeArguments().get(1).getName();
			Validator.validateTableTrue(realMapKeyType == expectedMapKeyType,
					"Column %s' of table '%s' of type 'Map<%s,?>' should be of type 'Map<%s,?>' indeed", columnName,
					tableName, realMapKeyType, expectedMapKeyType);

			Validator.validateTableTrue(realMapValueType == expectedValueType,
					"Column '%s' of table '%s' of type 'Map<?,%s>' should be of type 'Map<?,%s>' indeed", columnName,
					tableName, realMapValueType, expectedValueType);
			break;
		default:
			break;
		}
	}

	private void validatePrimaryKeyComponents(TableMetadata tableMetadata, PropertyMeta idMeta, boolean partitionKey) {

		log.debug("Validate existing primary key component from table {} against Achilles meta data {}",
				tableMetadata.getName(), idMeta);

		List<String> componentNames;
		List<Class<?>> componentClasses;
		if (partitionKey) {
			componentNames = idMeta.getPartitionComponentNames();
			componentClasses = idMeta.getPartitionComponentClasses();
		} else {
			componentNames = idMeta.getClusteringComponentNames();
			componentClasses = idMeta.getClusteringComponentClasses();
		}

		for (int i = 0; i < componentNames.size(); i++) {
			Class<?> componentClass = componentClasses.get(i);
			String componentName = componentNames.get(i);
			if (idMeta.isComponentTimeUUID(componentName)) {
				componentClass = InternalTimeUUID.class;
			}
			if (partitionKey)
				validatePartitionComponent(tableMetadata, componentName.toLowerCase(), componentClass);
			else
				validateClusteringComponent(tableMetadata, componentName.toLowerCase(), componentClass);
		}
	}

	private boolean hasColumnMeta(Collection<ColumnMetadata> columnMetadatas, ColumnMetadata fqcnColumn) {
		boolean fqcnColumnMatches = false;
		for (ColumnMetadata columnMetadata : columnMetadatas) {
			fqcnColumnMatches = fqcnColumnMatches || columnMetaDataComparator.isEqual(fqcnColumn, columnMetadata);
		}
		return fqcnColumnMatches;
	}
}
