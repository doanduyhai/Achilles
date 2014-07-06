package info.archinnov.achilles.internal.table;

import static com.datastax.driver.core.DataType.counter;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.map;
import static com.datastax.driver.core.DataType.set;
import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLDataType;
import static info.archinnov.achilles.internal.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.schemabuilder.SchemaBuilder.alterTable;
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
    private static final Logger DML_LOG = LoggerFactory.getLogger(TableCreator.ACHILLES_DDL_SCRIPT);

    private static final Function<ColumnMetadata, String> COLUMN_NAME_EXTRACTOR = new Function<ColumnMetadata, String>() {
        @Override
        public String apply(ColumnMetadata columnMetadata) {
            return columnMetadata.getName();
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
        addNewPropertiesToBuilder(session, entityMeta,propertyMetas, columnNames);
    }

    private void addNewPropertiesToBuilder(Session session,EntityMeta entityMeta,List<PropertyMeta> propertyMetas, Set<String> columnNames) {
        final String tableName = normalizerAndValidateColumnFamilyName(entityMeta.getTableName());

        for (PropertyMeta propertyMeta : propertyMetas) {
            if (!columnNames.contains(propertyMeta.getPropertyName())) {
                String propertyName = propertyMeta.getCQL3PropertyName();
                Class<?> keyClass = propertyMeta.getKeyClass();
                Class<?> valueClass = propertyMeta.getValueClassForTableCreation();
                final boolean staticColumn = propertyMeta.isStaticColumn();
                String alterTableScript = "";
                switch (propertyMeta.type()) {
                    case SIMPLE:
                        alterTableScript = alterTable(tableName).addColumn(propertyName, staticColumn).type(toCQLDataType(valueClass));
                        session.execute(alterTableScript);
                        break;
                    case LIST:
                        alterTableScript = alterTable(tableName).addColumn(propertyName, staticColumn).type(list(toCQLDataType(valueClass)));
                        session.execute(alterTableScript);
                        break;
                    case SET:
                        alterTableScript = alterTable(tableName).addColumn(propertyName, staticColumn).type(set(toCQLDataType(valueClass)));
                        session.execute(alterTableScript);
                        break;
                    case MAP:
                        alterTableScript = alterTable(tableName).addColumn(propertyName, staticColumn).type(map(toCQLDataType(keyClass), toCQLDataType(valueClass)));
                        session.execute(alterTableScript);
                        break;
                    case COUNTER:
                        if (entityMeta.isClusteredCounter()) {
                            alterTableScript = alterTable(tableName).addColumn(propertyName, staticColumn).type(counter());
                            session.execute(alterTableScript);
                        }
                        break;
                    default:
                        break;
                }

                if (StringUtils.isNotBlank(alterTableScript)) {
                    DML_LOG.debug(alterTableScript);
                }

                if (propertyMeta.isIndexed()) {
                    final String optionalIndexName = propertyMeta.getIndexProperties().getIndexName();
                    final String indexName = isBlank(optionalIndexName) ? tableName + "_" + propertyName : optionalIndexName;
                    final String createIndexScript = SchemaBuilder.createIndex(indexName).onTable(tableName).andColumn(propertyName);
                    session.execute(createIndexScript);
                    DML_LOG.debug(createIndexScript);
                }
            }
        }
    }
}
