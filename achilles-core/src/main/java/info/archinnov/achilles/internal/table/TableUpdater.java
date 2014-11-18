package info.archinnov.achilles.internal.table;

import static com.datastax.driver.core.DataType.counter;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.map;
import static com.datastax.driver.core.DataType.set;
import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLDataType;
import static info.archinnov.achilles.schemabuilder.SchemaBuilder.alterTable;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Function;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

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

        if (!entityMeta.config().isSchemaUpdateEnabled()) {
            return;
        }

        List<ColumnMetadata> existingColumns = tableMetadata.getColumns();
        List<PropertyMeta> propertyMetas = entityMeta.getAllMetasExceptId();
        Set<String> columnNames = from(existingColumns).transform(COLUMN_NAME_EXTRACTOR).toSet();
        addNewPropertiesToBuilder(session, entityMeta,propertyMetas, columnNames);
    }

    private void addNewPropertiesToBuilder(Session session,EntityMeta entityMeta,List<PropertyMeta> propertyMetas, Set<String> columnNames) {
        final String tableName = entityMeta.config().getQualifiedTableName();

        for (PropertyMeta propertyMeta : propertyMetas) {
            String cql3ColumnName = propertyMeta.getCQL3ColumnName();
            if (!columnNames.contains(cql3ColumnName)) {
                Class<?> valueClass = propertyMeta.structure().getCQL3ValueType();
                final boolean staticColumn = propertyMeta.structure().isStaticColumn();
                String alterTableScript = "";
                switch (propertyMeta.type()) {
                    case SIMPLE:
                        alterTableScript = alterTable(tableName).addColumn(cql3ColumnName, staticColumn).type(toCQLDataType(valueClass));
                        session.execute(alterTableScript);
                        break;
                    case LIST:
                        alterTableScript = alterTable(tableName).addColumn(cql3ColumnName, staticColumn).type(list(toCQLDataType(valueClass)));
                        session.execute(alterTableScript);
                        break;
                    case SET:
                        alterTableScript = alterTable(tableName).addColumn(cql3ColumnName, staticColumn).type(set(toCQLDataType(valueClass)));
                        session.execute(alterTableScript);
                        break;
                    case MAP:
                        final Class<Object> keyClass = propertyMeta.structure().getCQL3KeyType();
                        alterTableScript = alterTable(tableName).addColumn(cql3ColumnName, staticColumn).type(map(toCQLDataType(keyClass), toCQLDataType(valueClass)));
                        session.execute(alterTableScript);
                        break;
                    case COUNTER:
                        if (entityMeta.structure().isClusteredCounter()) {
                            alterTableScript = alterTable(tableName).addColumn(cql3ColumnName, staticColumn).type(counter());
                            session.execute(alterTableScript);
                        }
                        break;
                    default:
                        break;
                }

                if (StringUtils.isNotBlank(alterTableScript)) {
                    DML_LOG.debug(alterTableScript);
                }

                if (propertyMeta.structure().isIndexed()) {
                    final String createIndexScript = propertyMeta.forTableCreation().createNewIndexScript(tableName);
                    session.execute(createIndexScript);
                    DML_LOG.debug(createIndexScript);
                }
            }
        }
    }

    public static enum Singleton {
        INSTANCE;

        private final TableUpdater instance = new TableUpdater();

        public TableUpdater get() {
            return instance;
        }
    }
}
