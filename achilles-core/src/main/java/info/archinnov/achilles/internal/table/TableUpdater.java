package info.archinnov.achilles.internal.table;

import static com.datastax.driver.core.DataType.counter;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.map;
import static com.datastax.driver.core.DataType.set;
import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLDataType;
import static info.archinnov.achilles.internal.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static org.apache.commons.lang.StringUtils.isBlank;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.internal.cql.TypeMapper;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.schemabuilder.Alter;
import info.archinnov.achilles.schemabuilder.SchemaBuilder;

public class TableUpdater {
    private static final Logger log = LoggerFactory.getLogger(TableUpdater.class);

    private static final Function<ColumnMetadata, String> COLUMN_NAME_EXTRACTOR = new Function<ColumnMetadata, String>() {
        @Override
        public String apply(ColumnMetadata columnMetadata) {
            return columnMetadata.getName();
        }
    };

    private static final Predicate<String> NOT_BLANK = new Predicate<String>() {
        @Override
        public boolean apply(String input) {
            return StringUtils.isNotBlank(input);
        }
    };

    public void updateTableForEntity(Session session, EntityMeta entityMeta, TableMetadata tableMetadata) {
        log.debug("Updating table for entityMeta {}", entityMeta.getClassName());

        if (!entityMeta.isSchemaUpdateEnabled()) {
            return;
        }

        List<ColumnMetadata> existingColumns = tableMetadata.getColumns();
        List<PropertyMeta> propertyMetas = entityMeta.getAllMetasExceptId();
        Set<String> columnNames = from(existingColumns).transform(COLUMN_NAME_EXTRACTOR).toSet();

        final String tableName = normalizerAndValidateColumnFamilyName(tableMetadata.getName());
        final Alter alterTable = SchemaBuilder.alterTable(tableName);
        addNewPropertiesToBuilder(session, tableName,propertyMetas, columnNames, alterTable);
    }

    private void addNewPropertiesToBuilder(Session session,String tableName,List<PropertyMeta> propertyMetas, Set<String> columnNames, Alter alterTable) {
        for (PropertyMeta propertyMeta : propertyMetas) {
            if (!columnNames.contains(propertyMeta.getPropertyName())) {
                String propertyName = propertyMeta.getCQL3PropertyName();
                Class<?> keyClass = propertyMeta.getKeyClass();
                Class<?> valueClass = propertyMeta.getValueClassForTableCreation();
                switch (propertyMeta.type()) {
                    case SIMPLE:
                        session.execute(alterTable.addColumn(propertyName).type(toCQLDataType(valueClass)));
                        break;
                    case LIST:
                        session.execute(alterTable.addColumn(propertyName).type(list(toCQLDataType(valueClass))));
                        break;
                    case SET:
                        session.execute(alterTable.addColumn(propertyName).type(set(toCQLDataType(valueClass))));
                        break;
                    case MAP:
                        session.execute(alterTable.addColumn(propertyName).type(map(toCQLDataType(keyClass),toCQLDataType(valueClass))));
                        break;
                    case COUNTER:
                        session.execute(alterTable.addColumn(propertyName).type(counter()));
                        break;
                    default:
                        break;
                }
                if (propertyMeta.isIndexed()) {
                    final String optionalIndexName = propertyMeta.getIndexProperties().getIndexName();
                    final String indexName = isBlank(optionalIndexName) ? tableName + "_" + propertyName : optionalIndexName;
                    session.execute(SchemaBuilder.createIndex(indexName).onTable(tableName).andColumn(propertyName));
                }
            }
        }
    }
}
