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

import static com.datastax.driver.core.DataType.Name.COUNTER;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLType;
import static info.archinnov.achilles.internal.table.TableCreator.ACHILLES_DDL_SCRIPT;
import static info.archinnov.achilles.internal.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.internal.metadata.holder.IndexProperties;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.Pair;

public class TableBuilder extends AbstractTableBuilder {

	private static final Logger log = LoggerFactory.getLogger(ACHILLES_DDL_SCRIPT);

	private String tableName;
	private String comment;
	private List<String> partitionComponents = new ArrayList<>();
	private List<String> clusteringComponents = new ArrayList<>();
	private Map<String, String> columns = new LinkedHashMap<>();
	private Map<String, String> lists = new LinkedHashMap<>();
	private Map<String, String> sets = new LinkedHashMap<>();
	private Map<String, Pair<String, String>> maps = new LinkedHashMap<>();
	private String reversedComponent = null;
	private boolean counter;

	public static TableBuilder createTable(String tableName) {
		return new TableBuilder(tableName, false);
	}

	public static TableBuilder createCounterTable(String tableName) {
		return new TableBuilder(tableName, true);
	}

	private TableBuilder(String tableName, boolean counter) {
		this.counter = counter;
		this.tableName = normalizerAndValidateColumnFamilyName(tableName);
	}

	public TableBuilder addColumn(String columnName, Class<?> javaType) {
		columns.put(columnName, toCQLType(javaType).toString());
		return this;
	}

	public TableBuilder addList(String listName, Class<?> javaValueType) {
		lists.put(listName, toCQLType(javaValueType).toString());
		return this;
	}

	public TableBuilder addSet(String setName, Class<?> javaValueType) {
		sets.put(setName, toCQLType(javaValueType).toString());
		return this;
	}

	public TableBuilder addMap(String mapName, Class<?> javaKeyType, Class<?> javaValueType) {
		maps.put(mapName, Pair.create(toCQLType(javaKeyType).toString(), toCQLType(javaValueType).toString()));
		return this;
	}

	public TableBuilder addPartitionComponent(String columnName) {
		Validator.validateFalse(lists.containsKey(columnName),
				"Partition component '%s' for table '%s' cannot be of list type", columnName, tableName);
		Validator.validateFalse(sets.containsKey(columnName),
				"Partition component '%s' for table '%s' cannot be of set type", columnName, tableName);
		Validator.validateFalse(maps.containsKey(columnName),
				"Partition component '%s' for table '%s' cannot be of map type", columnName, tableName);

		Validator.validateTrue(columns.containsKey(columnName),
				"Property '%s' for table '%s' cannot be found. Did you forget to declare it as column first ?",
				columnName, tableName);

		partitionComponents.add(columnName);

		return this;
	}

	public TableBuilder addClusteringComponent(String columnName) {
		Validator.validateFalse(lists.containsKey(columnName),
				"Clustering component '%s' for table '%s' cannot be of list type", columnName, tableName);
		Validator.validateFalse(sets.containsKey(columnName),
				"Clustering component '%s' for table '%s' cannot be of set type", columnName, tableName);
		Validator.validateFalse(maps.containsKey(columnName),
				"Clustering component '%s' for table '%s' cannot be of map type", columnName, tableName);

		Validator.validateTrue(columns.containsKey(columnName),
				"Property '%s' for table '%s' cannot be found. Did you forget to declare it as column first ?",
				columnName, tableName);

		clusteringComponents.add(columnName);

		return this;
	}

	public TableBuilder addIndex(IndexProperties indexProperties) {
		String columnName = indexProperties.getPropertyName();
		Validator.validateFalse(lists.containsKey(columnName), "Index '%s' for table '%s' cannot be of list type",
				columnName, tableName);
		Validator.validateFalse(sets.containsKey(columnName), "Index '%s' for table '%s' cannot be of set type",
				columnName, tableName);
		Validator.validateFalse(maps.containsKey(columnName), "Index '%s' for table '%s' cannot be of map type",
				columnName, tableName);
		Validator.validateFalse(partitionComponents.contains(columnName),
				"Index '%s' for table '%s' cannot be a partition key component", columnName, tableName);
		Validator.validateFalse(clusteringComponents.contains(columnName),
				"Index '%s' for table '%s' cannot be a clustering key component", columnName, tableName);
		Validator.validateFalse(counter, "Index '%s' for table '%s' cannot be set on a counter table", columnName,
				tableName);

		Validator.validateTrue(columns.containsKey(columnName),
				"Property '%s' for table '%s' cannot be found. Did you forget to declare it as column first ?",
				columnName, tableName);

		indexedColumns.add(indexProperties);

		return this;
	}

	public TableBuilder addComment(String comment) {
		Validator.validateNotBlank(comment, "Comment for table '%s' should not be blank", tableName);
		this.comment = comment.replaceAll("'", "\"");
		return this;
	}

	public TableBuilder setReversedClusteredComponent(String reversedComponent) {
		this.reversedComponent = reversedComponent;
		return this;
	}

	public String generateDDLScript() {

		String ddlScript;
		if (counter) {
			ddlScript = generateCounterTable();
		} else {
			ddlScript = generateTable();
		}

		log.debug(ddlScript);

		return ddlScript;
	}

	private String generateTable() {
		StringBuilder ddl = new StringBuilder();

		ddl.append("\n");
		ddl.append("\tCREATE TABLE ");
		ddl.append(tableName).append("(\n");

		for (Entry<String, String> columnEntry : columns.entrySet()) {
			ddl.append("\t\t");
			ddl.append(columnEntry.getKey());
			ddl.append(" ");
			ddl.append(columnEntry.getValue());
			ddl.append(",\n");
		}
		for (Entry<String, String> listEntry : lists.entrySet()) {
			ddl.append("\t\t");
			ddl.append(listEntry.getKey());
			ddl.append(" list<");
			ddl.append(listEntry.getValue());
			ddl.append(">");
			ddl.append(",\n");
		}
		for (Entry<String, String> setEntry : sets.entrySet()) {
			ddl.append("\t\t");
			ddl.append(setEntry.getKey());
			ddl.append(" set<");
			ddl.append(setEntry.getValue());
			ddl.append(">");
			ddl.append(",\n");
		}
		for (Entry<String, Pair<String, String>> mapEntry : maps.entrySet()) {
			ddl.append("\t\t");
			ddl.append(mapEntry.getKey());
			ddl.append(" map<");
			ddl.append(mapEntry.getValue().left);
			ddl.append(",");
			ddl.append(mapEntry.getValue().right);
			ddl.append(">");
			ddl.append(",\n");
			;
		}

		ddl.append("\t\t");
		ddl.append("PRIMARY KEY(");

		if (partitionComponents.size() > 1)
			ddl.append("(").append(StringUtils.join(partitionComponents, ", ")).append(")");
		else
			ddl.append(partitionComponents.get(0));

		if (clusteringComponents.size() > 0) {
			ddl.append(", ");
			ddl.append(StringUtils.join(clusteringComponents, ", "));
		}
		ddl.append(")\n");

		ddl.append("\t)");

		// Add comments
		ddl.append(" WITH COMMENT = '").append(comment).append("'");
		if (reversedComponent != null) {
			ddl.append(" AND CLUSTERING ORDER BY (").append(reversedComponent).append(" DESC)");
		}
		return ddl.toString();
	}

	public Collection<String> generateIndices() {
		return generateIndices(indexedColumns, tableName);
	}

	private String generateCounterTable() {

		StringBuilder ddl = new StringBuilder();

		ddl.append("\n");
		ddl.append("\tCREATE TABLE ");
		ddl.append(tableName).append("(\n");

		for (Entry<String, String> columnEntry : columns.entrySet()) {
			String columnName = columnEntry.getKey();
			String valueType = columnEntry.getValue();

			ddl.append("\t\t");
			ddl.append(columnName);
			ddl.append(" ");
			if (partitionComponents.contains(columnName) || clusteringComponents.contains(columnName)) {
				ddl.append(valueType);
			} else {
				Validator.validateTrue(StringUtils.equals(valueType, COUNTER.toString()),
						"Column '%s' of table '%s' should be of type 'counter'", columnName, tableName);
				ddl.append("counter");
			}
			ddl.append(",\n");
		}

		ddl.append("\t\t");
		ddl.append("PRIMARY KEY(");

		if (partitionComponents.size() > 1)
			ddl.append("(").append(StringUtils.join(partitionComponents, ", ")).append(")");
		else
			ddl.append(partitionComponents.get(0));

		if (clusteringComponents.size() > 0) {
			ddl.append(", ");
			ddl.append(StringUtils.join(clusteringComponents, ", "));
		}

		ddl.append(")\n");

		ddl.append("\t)");

		// Add comments
		ddl.append(" WITH COMMENT = '").append(comment).append("'");
		return ddl.toString();
	}
}
