package info.archinnov.achilles.internal.table;

import static com.google.common.collect.FluentIterable.from;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

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

        TableUpdateBuilder builder = new TableUpdateBuilder(tableMetadata.getName());
        addNewPropertiesToBuilder(propertyMetas, columnNames, builder);

        final List<String> updateScripts = FluentIterable.from(builder.generateDDLUpdateScripts()).filter(NOT_BLANK).toList();
        for (String updateScript : updateScripts) {
            session.execute(updateScript);
        }

        if (builder.hasIndices()) {
            for (String indexScript : builder.generateIndices()) {
                session.execute(indexScript);
            }
        }
    }

    private void addNewPropertiesToBuilder(List<PropertyMeta> propertyMetas, Set<String> columnNames, TableUpdateBuilder builder) {
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
                        builder.addMap(propertyName, keyClass, valueClass);
                        break;
                    case COUNTER:
                        builder.addCounter(propertyName);
                        break;
                    default:
                        break;
                }
                if (propertyMeta.isIndexed()) {
                    builder.addIndex(propertyMeta.getIndexProperties(), propertyMeta);
                }
            }
        }
    }
}
