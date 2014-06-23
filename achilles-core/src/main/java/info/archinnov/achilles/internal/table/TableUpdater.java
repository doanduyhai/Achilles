package info.archinnov.achilles.internal.table;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.IndexProperties;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

public class TableUpdater {
	private static final Logger log = LoggerFactory.getLogger(TableUpdater.class);

	public void updateTableForEntity(Session session, EntityMeta entityMeta, TableMetadata tableMetadata) {
		log.debug("Updating table for entityMeta {}", entityMeta.getClassName());

		if (!entityMeta.isSchemaUpdateEnabled()) {
			return;
		}

		List<ColumnMetadata> existsColumns = tableMetadata.getColumns();
		List<PropertyMeta> propertyMetas = entityMeta.getAllMetasExceptIdAndCounters();
		Collection<String> namesCollection = Collections2.transform(existsColumns, new ColumnNameExtractFunction());
		Set<String> columnNames = new HashSet<>(namesCollection);

		TableUpdateBuilder builder = new TableUpdateBuilder(tableMetadata.getName());
		addNewPropertiesToBuilder(propertyMetas, columnNames, builder);

		addNewIndexesToBuilder(existsColumns, propertyMetas, builder);
		session.execute(builder.generateDDLScript());
		if (builder.hasIndices()) {
			for (String indexScript : builder.generateIndices()) {
				session.execute(indexScript);
			}
		}
	}

	private void addNewIndexesToBuilder(List<ColumnMetadata> existsColumns, List<PropertyMeta> propertyMetas,
			TableUpdateBuilder builder) {
		Collection<ColumnMetadata> columnsWithIndex = Collections2
				.filter(existsColumns, new ColumnMetaIndexPredicate());
		Collection<PropertyMeta> propertyWithIndex = Collections2.filter(propertyMetas,
				new PropertyMetaIndexPredicate());
		Collection<String> columnNamesWithIndex = Collections2.transform(columnsWithIndex,
				new ColumnNameExtractFunction());
		// name->indexed
		Map<String, PropertyMeta> propertyMap = Maps.uniqueIndex(propertyWithIndex, new PropertyNameExtractFunction());
		HashSet<String> newIndexes = new HashSet<>(columnNamesWithIndex);
		newIndexes.removeAll(propertyMap.keySet());

		for (String indexPropertyName : newIndexes) {
			PropertyMeta meta = propertyMap.get(indexPropertyName);
			IndexProperties indexProperties = meta.getIndexProperties();
			builder.addIndex(new IndexProperties(indexProperties.getName(), indexPropertyName), meta);
		}
	}

	private void addNewPropertiesToBuilder(List<PropertyMeta> propertyMetas, Set<String> columnNames,
			TableUpdateBuilder builder) {
		for (PropertyMeta propertyMeta : propertyMetas) {
			if (!columnNames.contains(propertyMeta.getPropertyName())) {
				String propertyName = propertyMeta.getPropertyName();
				Class<?> keyClass = propertyMeta.getKeyClass();
				Class<?> valueClass = propertyMeta.getValueClassForTableCreation();
				switch (propertyMeta.type()) {
				case SIMPLE:
					builder.addColumn(propertyName, valueClass);
					break;
				case LIST:
					builder.addList(propertyName, valueClass);
					break;
				case SET:
					builder.addSet(propertyName, valueClass);
					break;
				case MAP:
					builder.addMap(propertyName, keyClass, propertyMeta.getValueClass());
					break;
				default:
					break;
				}
				if (propertyMeta.isIndexed()) {
					IndexProperties indexProperties = new IndexProperties(propertyMeta.getIndexProperties().getName(),
							propertyName);
					builder.addIndex(indexProperties, propertyMeta);
				}
			}
		}
	}

	private static class ColumnNameExtractFunction implements Function<ColumnMetadata, String> {
		@Override
		public String apply(ColumnMetadata columnMetadata) {
			return columnMetadata.getName();
		}
	}

	private static class PropertyNameExtractFunction implements Function<PropertyMeta, String> {
		@Override
		public String apply(PropertyMeta input) {
			return input.getPropertyName();
		}
	}

	private static class PropertyMetaIndexPredicate implements Predicate<PropertyMeta> {
		@Override
		public boolean apply(PropertyMeta input) {
			return input.isIndexed();
		}
	}

	private static class ColumnMetaIndexPredicate implements Predicate<ColumnMetadata> {
		@Override
		public boolean apply(ColumnMetadata input) {
			return input.getIndex() != null;
		}
	}
}
