package info.archinnov.achilles.internal.metadata.holder;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.table.ColumnMetaDataComparator;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLType;

public class PropertyMetaTableValidator extends PropertyMetaView{

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaTableValidator.class);

    protected PropertyMetaTableValidator(PropertyMeta meta) {
        super(meta);
    }

    private ColumnMetaDataComparator columnMetaDataComparator = ColumnMetaDataComparator.Singleton.INSTANCE.get();

    public void validatePrimaryKeyComponents(TableMetadata tableMetadata, boolean partitionKey) {
        log.debug("Validate existing primary key component from table {} against entity class {}",tableMetadata.getName(), meta.getEntityClassName());
        Validator.validateNotNull(meta.getEmbeddedIdProperties(), "Cannot validate compound primary keys components against Cassandra meta data because entity '%s' does not have a compound primary key", meta.getEntityClassName());
        if (partitionKey) {
            for (PropertyMeta partitionMeta : meta.getEmbeddedIdProperties().getPartitionComponents().propertyMetas) {
                validatePartitionComponent(tableMetadata, partitionMeta);
            }
        } else {
            for (PropertyMeta clusteringMeta : meta.getEmbeddedIdProperties().getClusteringComponents().propertyMetas) {
                validateClusteringComponent(tableMetadata, clusteringMeta);
            }
        }
    }

    public void validateColumn(TableMetadata tableMetaData, EntityMeta entityMeta, ConfigurationContext configContext) {
        final String cql3ColumnName = meta.getCQL3ColumnName();
        final Class<?> columnJavaType = meta.structure().getCQL3ValueType();
        final boolean schemaUpdateEnabled = entityMeta.config().isSchemaUpdateEnabled();
        final String tableName = tableMetaData.getName();

        if (log.isDebugEnabled()) {
            log.debug("Validate existing column {} from table {} against type {}", cql3ColumnName, tableName, columnJavaType);
        }

        final ColumnMetadata columnMetadata = tableMetaData.getColumn(cql3ColumnName);


        if (schemaUpdateEnabled && columnMetadata == null) {
            // will be created in updater
            return;
        } else {
            Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", cql3ColumnName, tableName);
        }

        validateColumnType(tableName, cql3ColumnName, columnMetadata, columnJavaType);
        validateStatic(cql3ColumnName, tableName, columnMetadata);


        if (!configContext.isRelaxIndexValidation()) {
            boolean columnIsIndexed = columnMetadata.getIndex() != null;
            Validator.validateTableFalse((columnIsIndexed ^ meta.structure().isIndexed()),"Column '%s' in the table '%s' is indexed (or not) whereas metadata indicates it is (or not)",cql3ColumnName, tableName);
        }
    }

    public void validateCollectionAndMapColumn(TableMetadata tableMetadata, EntityMeta entityMeta) {
        final String cql3ColumnName = meta.getCQL3ColumnName();
        final String tableName = tableMetadata.getName();


        final boolean schemaUpdateEnabled = entityMeta.config().isSchemaUpdateEnabled();
        final ColumnMetadata columnMetadata = tableMetadata.getColumn(cql3ColumnName);

        if (schemaUpdateEnabled && columnMetadata == null) {
            // will be created in updater
            return;
        } else {
            Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", cql3ColumnName, tableName);
        }
        final Name realType = columnMetadata.getType().getName();

        if (log.isDebugEnabled()) {
            log.debug("Validate existing collection/map column {} from table {} against type {}", cql3ColumnName, tableName, realType);
        }

        final Name expectedValueType = toCQLType(meta.structure().getCQL3ValueType());

        switch (meta.type()) {
            case LIST:
                Validator.validateTableTrue(realType == Name.LIST,
                        "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", cql3ColumnName, tableName,
                        realType, Name.LIST);
                Name realListValueType = columnMetadata.getType().getTypeArguments().get(0).getName();
                Validator.validateTableTrue(realListValueType == expectedValueType,
                        "Column '%s' of table '%s' of type 'List<%s>' should be of type 'List<%s>' indeed", cql3ColumnName,
                        tableName, realListValueType, expectedValueType);

                break;
            case SET:
                Validator.validateTableTrue(realType == Name.SET,
                        "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", cql3ColumnName, tableName,
                        realType, Name.SET);
                Name realSetValueType = columnMetadata.getType().getTypeArguments().get(0).getName();

                Validator.validateTableTrue(realSetValueType == expectedValueType,
                        "Column '%s' of table '%s' of type 'Set<%s>' should be of type 'Set<%s>' indeed", cql3ColumnName,
                        tableName, realSetValueType, expectedValueType);
                break;
            case MAP:
                Validator.validateTableTrue(realType == Name.MAP,
                        "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", cql3ColumnName, tableName,
                        realType, Name.MAP);

                Name expectedMapKeyType = toCQLType(meta.structure().getCQL3KeyType());
                Name realMapKeyType = columnMetadata.getType().getTypeArguments().get(0).getName();
                Name realMapValueType = columnMetadata.getType().getTypeArguments().get(1).getName();
                Validator.validateTableTrue(realMapKeyType == expectedMapKeyType,
                        "Column %s' of table '%s' of type 'Map<%s,?>' should be of type 'Map<%s,?>' indeed", cql3ColumnName,
                        tableName, realMapKeyType, expectedMapKeyType);

                Validator.validateTableTrue(realMapValueType == expectedValueType,
                        "Column '%s' of table '%s' of type 'Map<?,%s>' should be of type 'Map<?,%s>' indeed", cql3ColumnName,
                        tableName, realMapValueType, expectedValueType);
                break;
            default:
                break;
        }
    }

    public void validateClusteredCounterColumn(TableMetadata tableMetaData, EntityMeta entityMeta) {
        final String cql3ColumnName = meta.getCQL3ColumnName();

        if (log.isDebugEnabled()) {
            log.debug("Validate existing counter column {} from table {} against type {}", cql3ColumnName, tableMetaData.getName(), Counter.class);
        }

        final boolean schemaUpdateEnabled = entityMeta.config().isSchemaUpdateEnabled();

        final String tableName = tableMetaData.getName();
        final ColumnMetadata columnMetadata = tableMetaData.getColumn(cql3ColumnName);

        if (schemaUpdateEnabled && columnMetadata == null) {
            // will be created in updater
            return;
        } else {
            Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", cql3ColumnName, tableName);
        }

        final Name realType = columnMetadata.getType().getName();

        Validator.validateTableTrue(realType == Name.COUNTER, "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", cql3ColumnName, tableName, realType, Name.COUNTER);
    }

    private void validateStatic(String cql3ColumnName, String tableName, ColumnMetadata columnMetadata) {
        Validator.validateBeanMappingTrue(columnMetadata.isStatic() == meta.isStaticColumn(), "Column '%s' of table '%s' is declared as static='%s' in Java but as static='%s' in Cassandra", cql3ColumnName, tableName, meta.isStaticColumn(), columnMetadata.isStatic());
    }

    private void validatePartitionComponent(TableMetadata tableMetaData, PropertyMeta partitionMeta) {
        final String tableName = tableMetaData.getName();
        final String cql3ColumnName = partitionMeta.getCQL3ColumnName();
        final Class<?> columnJavaType = partitionMeta.structure().getCQL3ValueType();

        if (log.isDebugEnabled()) {
            log.debug("Validate existing partition key component {} from table {} against type {}", cql3ColumnName, tableName, columnJavaType.getCanonicalName());
        }

        // no ALTER's for partition components
        ColumnMetadata columnMetadata = tableMetaData.getColumn(cql3ColumnName);
        Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", cql3ColumnName, tableName);
        validateColumnType(tableName, cql3ColumnName, columnMetadata, columnJavaType);

        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getPartitionKey(), columnMetadata),"Column '%s' of table '%s' should be a partition key component", cql3ColumnName, tableName);
    }


    private void validateClusteringComponent(TableMetadata tableMetaData, PropertyMeta clusteringMeta) {
        final String tableName = tableMetaData.getName();
        final String cql3ColumnName = clusteringMeta.getCQL3ColumnName();
        final Class<?> columnJavaType = clusteringMeta.structure().getCQL3ValueType();

        if (log.isDebugEnabled()) {
            log.debug("Validate existing clustering column {} from table {} against type {}", cql3ColumnName,tableName, columnJavaType);
        }

        // no ALTER's for clustering components
        ColumnMetadata columnMetadata = tableMetaData.getColumn(cql3ColumnName);
        Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", cql3ColumnName, tableName);
        validateColumnType(tableName, cql3ColumnName, columnMetadata, columnJavaType);

        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getClusteringColumns(), columnMetadata),"Column '%s' of table '%s' should be a clustering key component", cql3ColumnName, tableName);
    }

    private void validateColumnType(String tableName, String columnName, ColumnMetadata columnMetadata, Class<?> columnJavaType) {
        DataType.Name expectedType = toCQLType(columnJavaType);
        DataType.Name realType = columnMetadata.getType().getName();
		/*
         * See JIRA
		 */
        if (realType == DataType.Name.CUSTOM) {
            realType = DataType.Name.BLOB;
        }
        Validator.validateTableTrue(expectedType == realType, "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName, realType, expectedType);
    }

    private boolean hasColumnMeta(Collection<ColumnMetadata> columnMetadatas, ColumnMetadata columnMetaToVerify) {
        boolean fqcnColumnMatches = false;
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            fqcnColumnMatches = fqcnColumnMatches || columnMetaDataComparator.isEqual(columnMetaToVerify, columnMetadata);
        }
        return fqcnColumnMatches;
    }

}
