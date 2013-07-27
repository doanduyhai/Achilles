package info.archinnov.achilles.table;

import static com.datastax.driver.core.DataType.*;
import static info.archinnov.achilles.counter.AchillesCounter.*;
import static info.archinnov.achilles.cql.CQLTypeMapper.toCQLType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.validation.Validator;
import java.util.List;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

/**
 * CQLTableValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLTableValidator {

    private Cluster cluster;
    private String keyspaceName;

    public CQLTableValidator(Cluster cluster, String keyspaceName) {
        this.cluster = cluster;
        this.keyspaceName = keyspaceName;
    }

    public void validateForEntity(EntityMeta entityMeta, TableMetadata tableMetadata) {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        if (idMeta.isEmbeddedId())
        {
            List<String> componentNames = idMeta.getComponentNames();
            List<Class<?>> componentClasses = idMeta.getComponentClasses();
            for (int i = 0; i < componentNames.size(); i++)
            {
                validateColumn(tableMetadata, componentNames.get(i).toLowerCase(), componentClasses.get(i));
            }
        }
        else
        {
            validateColumn(tableMetadata, idMeta);
        }

        for (PropertyMeta<?, ?> pm : entityMeta.getAllMetasExceptIdMeta())
        {
            switch (pm.type())
            {
                case SIMPLE:
                case LAZY_SIMPLE:
                case JOIN_SIMPLE:
                    validateColumn(tableMetadata, pm);
                    break;
                case LIST:
                case SET:
                case MAP:
                case LAZY_LIST:
                case LAZY_SET:
                case LAZY_MAP:
                case JOIN_LIST:
                case JOIN_SET:
                case JOIN_MAP:
                    validateCollectionAndMapColumn(tableMetadata, pm);
                    break;
                default:
                    break;
            }
        }

    }

    public void validateForWideMap(EntityMeta meta, PropertyMeta<?, ?> pm, TableMetadata tableMetadata) {
        // TODO Auto-generated method stub

    }

    public void validateAchillesCounter() {
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspaceName);
        TableMetadata tableMetadata = keyspaceMetadata.getTable(CQL_COUNTER_TABLE);
        Validator.validateTableTrue(tableMetadata != null, "Cannot find table '" + CQL_COUNTER_TABLE
                + "' from keyspace '" + keyspaceName);

        ColumnMetadata fqcnColumn = tableMetadata.getColumn(CQL_COUNTER_FQCN);
        Validator.validateTableTrue(fqcnColumn != null, "Cannot find column '" + CQL_COUNTER_FQCN + "' from table '"
                + CQL_COUNTER_TABLE);
        Validator.validateTableTrue(fqcnColumn.getType() == text(), "Column '" + CQL_COUNTER_FQCN
                + "' of type '" + fqcnColumn.getType() + "' should be of type '" + text());

        ColumnMetadata pkColumn = tableMetadata.getColumn(CQL_COUNTER_PRIMARY_KEY);
        Validator.validateTableTrue(pkColumn != null, "Cannot find column '" + CQL_COUNTER_PRIMARY_KEY
                + "' from table '" + CQL_COUNTER_TABLE);
        Validator.validateTableTrue(pkColumn.getType() == text(), "Column '" + CQL_COUNTER_PRIMARY_KEY
                + "' of type '" + pkColumn.getType() + "' should be of type '" + text());

        ColumnMetadata propertyNameColumn = tableMetadata.getColumn(CQL_COUNTER_PROPERTY_NAME);
        Validator.validateTableTrue(propertyNameColumn != null, "Cannot find column '" + CQL_COUNTER_PROPERTY_NAME
                + "' from table '" + CQL_COUNTER_TABLE);
        Validator.validateTableTrue(propertyNameColumn.getType() == text(), "Column '"
                + CQL_COUNTER_PROPERTY_NAME + "' of type '" + propertyNameColumn.getType()
                + "' should be of type '" + text());

        ColumnMetadata counterValueColumn = tableMetadata.getColumn(CQL_COUNTER_VALUE);
        Validator.validateTableTrue(counterValueColumn != null, "Cannot find column '" + counterValueColumn
                + "' from table '" + CQL_COUNTER_TABLE);
        Validator.validateTableTrue(counterValueColumn.getType() == counter(), "Column '"
                + counterValueColumn + "' of type '" + counterValueColumn.getType()
                + "' should be of type '" + counter());

    }

    private void validateColumn(TableMetadata tableMetadata, PropertyMeta<?, ?> pm)
    {
        if (pm.isJoin())
        {
            validateColumn(tableMetadata, pm.getPropertyName().toLowerCase(), pm.joinIdMeta().getValueClass());
        }
        else
        {
            validateColumn(tableMetadata, pm.getPropertyName().toLowerCase(), pm.getValueClass());
        }
    }

    private void validateColumn(TableMetadata tableMetadata, String columnName, Class<?> columnJavaType)
    {
        String tableName = tableMetadata.getName();
        ColumnMetadata columnMetadata = tableMetadata.getColumn(columnName);
        Name expectedType = toCQLType(columnJavaType);

        Validator.validateTableTrue(columnMetadata != null, "Cannot find column '" + columnName
                + "' in the table '" + tableName + "'");

        Name realType = columnMetadata.getType().getName();
        Validator.validateTableTrue(expectedType == realType, "Column '" + columnName + "' of table '"
                + tableName + "' of type '" + realType + "' should be of type '" + expectedType + "' indeed");
    }

    private void validateCollectionAndMapColumn(TableMetadata tableMetadata, PropertyMeta<?, ?> pm)
    {
        String columnName = pm.getPropertyName().toLowerCase();
        String tableName = tableMetadata.getName();
        ColumnMetadata columnMetadata = tableMetadata.getColumn(columnName);

        Validator.validateTableTrue(columnMetadata != null, "Cannot find column '" + columnName
                + "' in the table '" + tableName + "'");
        Name realType = columnMetadata.getType().getName();
        Name expectedValueType;
        if (pm.isJoin())
        {
            expectedValueType = toCQLType(pm.joinIdMeta().getValueClass());
        }
        else
        {
            expectedValueType = toCQLType(pm.getValueClass());
        }

        switch (pm.type())
        {
            case LIST:
                Validator.validateTableTrue(realType == Name.LIST, "Column '" + columnName + "' of table '"
                        + tableName + "' of type '" + realType + "' should be of type '" + Name.LIST + "' indeed");
                Name realListValueType = columnMetadata.getType().getTypeArguments().get(0).getName();
                Validator.validateTableTrue(
                        realListValueType == expectedValueType,
                        "Column '" + columnName + "' of table '" + tableName + "' of type 'List<" + realListValueType
                                + ">' should be of type 'List<" + expectedValueType + "'> indeed");

                break;
            case SET:
                Validator.validateTableTrue(realType == Name.SET, "Column '" + columnName + "' of table '"
                        + tableName + "' of type '" + realType + "' should be of type '" + Name.SET + "' indeed");
                Name realSetValueType = columnMetadata.getType().getTypeArguments().get(0).getName();
                Validator.validateTableTrue(
                        realSetValueType == expectedValueType,
                        "Column '" + columnName + "' of table '" + tableName + "' of type 'Set<" + realSetValueType
                                + ">' should be of type 'Set<" + expectedValueType + "'> indeed");
                break;
            case MAP:
                Validator.validateTableTrue(realType == Name.MAP, "Column '" + columnName + "' of table '"
                        + tableName + "' of type '" + realType + "' should be of type '" + Name.MAP + "' indeed");
                Name expectedMapKeyType = toCQLType(pm.getKeyClass());
                Name realMapKeyType = columnMetadata.getType().getTypeArguments().get(0).getName();
                Name realMapValueType = columnMetadata.getType().getTypeArguments().get(1).getName();
                Validator.validateTableTrue(
                        realMapKeyType == expectedMapKeyType,
                        "Column '" + columnName + "' of table '" + tableName + "' of type 'Map<" + realMapKeyType
                                + ",?>' should be of type 'Map<" + expectedMapKeyType + "',?> indeed");
                Validator.validateTableTrue(
                        realMapValueType == expectedValueType,
                        "Column '" + columnName + "' of table '" + tableName + "' of type 'Map<?," + realMapValueType
                                + ">' should be of type 'Map<?," + expectedValueType + "'> indeed");
                break;
            default:
                break;
        }
    }

}
