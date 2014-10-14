package info.archinnov.achilles.internal.table;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaReader {

    private static final Logger log = LoggerFactory.getLogger(SchemaReader.class);

    private static final Function<EntityMeta, KeyspaceAndTable> EXTRACT_KEYSPACE_AND_TABLE = new Function<EntityMeta, KeyspaceAndTable>() {
        @Override
        public KeyspaceAndTable apply(EntityMeta meta) {
            return new KeyspaceAndTable(meta.config().getKeyspaceName(),
                    meta.config().getTableName(),
                    meta.config().getQualifiedTableName());
        }
    };

    public Map<String, TableMetadata> fetchTableMetaData(Cluster cluster, Collection<EntityMeta> entityMetas) {

        log.debug("Fetch existing table meta data from Cassandra");
        final Metadata clusterMetadata = cluster.getMetadata();
        final List<KeyspaceAndTable> keyspaceAndTables = FluentIterable.from(entityMetas).transform(EXTRACT_KEYSPACE_AND_TABLE).toList();

        Map<String, TableMetadata> tableMetas = new HashMap<>();

        for (KeyspaceAndTable keyspaceAndTable : keyspaceAndTables) {
            final KeyspaceMetadata keyspaceMetadata = clusterMetadata.getKeyspace(keyspaceAndTable.keyspaceName);
            Validator.validateTableTrue(keyspaceMetadata != null, "Keyspace '%s' doest not exist or cannot be found", keyspaceAndTable.keyspaceName);

            final TableMetadata tableMetadata = keyspaceMetadata.getTable(keyspaceAndTable.tableName);
            if (tableMetadata != null) {
                tableMetas.put(keyspaceAndTable.qualifiedTableName, tableMetadata);
            }
        }
        return tableMetas;
    }

    private static class KeyspaceAndTable {
        String keyspaceName;
        String tableName;
        String qualifiedTableName;

        private KeyspaceAndTable(String keyspaceName, String tableName, String qualifiedTableName) {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.qualifiedTableName = qualifiedTableName;
        }
    }

    public static enum Singleton {
        INSTANCE;

        private final SchemaReader instance = new SchemaReader();

        public SchemaReader get() {
            return instance;
        }
    }
}
