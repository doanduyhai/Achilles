package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.schemabuilder.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLDataType;
import static info.archinnov.achilles.schemabuilder.SchemaBuilder.createIndex;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class PropertyMetaTableCreator extends PropertyMetaView {
    private static final Logger log = LoggerFactory.getLogger(PropertyMetaTableCreator.class);

    protected PropertyMetaTableCreator(PropertyMeta meta) {
        super(meta);
    }

    public void addPartitionKeys(Create createTable) {
        log.debug("Adding partition keys {} for entity class {}", meta.getEmbeddedIdProperties().getPartitionComponents().getCQL3ComponentNames(), meta.getEntityClassName());
        Validator.validateNotNull(meta.getEmbeddedIdProperties(), "Cannot create partition components for entity '%s' because it does not have a compound primary key", meta.getEntityClassName());
        for (PropertyMeta partitionMeta: meta.getEmbeddedIdProperties().getPartitionComponents().propertyMetas) {
            String cql3ColumnName = partitionMeta.getCQL3ColumnName();
            Class<?> javaType = partitionMeta.structure().getCQL3ValueType();
            createTable.addPartitionKey(cql3ColumnName, toCQLDataType(javaType));
        }
    }

    public void addClusteringKeys(Create createTable) {
        log.debug("Adding clustering keys {} for entity class {}", meta.getEmbeddedIdProperties().getClusteringComponents().getCQL3ComponentNames(), meta.getEntityClassName());
        Validator.validateNotNull(meta.getEmbeddedIdProperties(), "Cannot create clustering keys for entity '%s' because it does not have a compound primary key",meta.getEntityClassName());
        for (PropertyMeta clusteringMeta: meta.getEmbeddedIdProperties().getClusteringComponents().propertyMetas) {
            String cql3ColumnName = clusteringMeta.getCQL3ColumnName();
            Class<?> javaType = clusteringMeta.structure().getCQL3ValueType();
            createTable.addClusteringKey(cql3ColumnName, toCQLDataType(javaType));
        }
    }

    public String createNewIndexScript(String tableName) {
        Validator.validateNotNull(meta.getIndexProperties(), "Cannot create new index script on property {} of entity {} because it is not defined as indexed",meta.propertyName, meta.getEntityClassName());
        log.debug("Creating new index {} for table {}", meta.getIndexProperties().getIndexName(), tableName);
        final String optionalIndexName = meta.getIndexProperties().getIndexName();
        final String cql3ColumnName = meta.getCQL3ColumnName();
        final String indexName = isBlank(optionalIndexName) ? tableName + "_" + cql3ColumnName + "_idx" : optionalIndexName;
        return createIndex(indexName).onTable(tableName).andColumn(cql3ColumnName);
    }

    public Create.Options addClusteringOrder(Create.Options tableOptions) {
        if (meta.structure().isClustered()) {
            final List<Create.Options.ClusteringOrder> clusteringOrders = meta.getEmbeddedIdProperties().getClusteringComponents().getClusteringOrders();
            log.debug("Add clustering orders {} to entity class {}", clusteringOrders, meta.getEntityClassName());
            return tableOptions.clusteringOrder(clusteringOrders.toArray(new Create.Options.ClusteringOrder[clusteringOrders.size()]));
        }
        return tableOptions;
    }
}
