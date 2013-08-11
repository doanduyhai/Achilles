package info.archinnov.achilles.table;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static info.archinnov.achilles.table.TableCreator.ACHILLES_DDL_SCRIPT;
import static me.prettyprint.hector.api.ddl.ComparatorType.*;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftColumnFamilyFactory {

    public static final String ENTITY_COMPARATOR_TYPE_ALIAS = "(org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";
    public static final String ENTITY_COMPARATOR_TYPE_CHECK = "CompositeType(org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";

    public static final String COUNTER_KEY_ALIAS = "(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)";
    public static final String COUNTER_KEY_CHECK = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)";
    public static final String COUNTER_COMPARATOR_TYPE_ALIAS = "(org.apache.cassandra.db.marshal.UTF8Type)";
    public static final String COUNTER_COMPARATOR_CHECK = "CompositeType(org.apache.cassandra.db.marshal.UTF8Type)";

    protected static final Logger log = LoggerFactory.getLogger(ACHILLES_DDL_SCRIPT);

    private ThriftComparatorTypeAliasFactory comparatorAliasFactory = new ThriftComparatorTypeAliasFactory();

    public ColumnFamilyDefinition createEntityCF(EntityMeta entityMeta, String keyspaceName) {

        String entityName = entityMeta.getClassName();
        String columnFamilyName = entityMeta.getTableName();

        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamilyName,
                ComparatorType.COMPOSITETYPE);

        Serializer<?> idSerializer = ThriftSerializerTypeInferer.getSerializer(entityMeta.getIdClass());
        String keyValidationType = idSerializer.getComparatorType().getTypeName();

        cfDef.setKeyValidationClass(keyValidationType);
        cfDef.setComparatorTypeAlias(ENTITY_COMPARATOR_TYPE_ALIAS);
        cfDef.setDefaultValidationClass(STRING_SRZ.getComparatorType().getTypeName());
        cfDef.setComment("Column family for entity '" + entityName + "'");

        StringBuilder builder = new StringBuilder("\n\n");
        builder.append("Create column family for entity ");
        builder.append("'").append(entityName).append("' : \n");
        builder.append("\tcreate column family ").append(columnFamilyName).append("\n");
        builder.append("\t\twith key_validation_class = ").append(keyValidationType).append("\n");
        builder.append("\t\tand comparator = '").append(ENTITY_COMPARATOR_TYPE_CHECK).append("'\n");
        builder.append("\t\tand default_validation_class = ").append(ComparatorType.UTF8TYPE.getTypeName())
                .append("\n");
        builder.append("\t\tand comment = 'Column family for entity ").append(entityName).append("'\n\n");

        log.debug(builder.toString());

        return cfDef;
    }

    public ColumnFamilyDefinition createClusteredEntityCF(String keyspaceName, EntityMeta entityMeta) {

        String tableName = entityMeta.getTableName();
        String entityName = entityMeta.getClassName();

        PropertyMeta idMeta = entityMeta.getIdMeta();
        Class<?> keyClass = idMeta.getComponentClasses().get(0);
        String defaultValidationType;
        if (entityMeta.isValueless())
        {
            defaultValidationType = STRING_SRZ.getComparatorType().getTypeName();
        }
        else
        {
            PropertyMeta pm = entityMeta.getFirstMeta();
            Class<?> valueClass = pm.getValueClass();
            Serializer<?> valueSerializer;
            if (pm.isCounter()) {
                valueSerializer = LONG_SRZ;
                defaultValidationType = COUNTERTYPE.getTypeName();
            } else if (pm.isJoin()) {
                valueSerializer = ThriftSerializerTypeInferer.getSerializer(pm.joinIdMeta().getValueClass());
                defaultValidationType = valueSerializer.getComparatorType().getTypeName();
            } else {
                valueSerializer = ThriftSerializerTypeInferer.getSerializer(valueClass);
                defaultValidationType = valueSerializer.getComparatorType().getTypeName();
            }
        }

        Serializer<?> keySerializer = ThriftSerializerTypeInferer.getSerializer(keyClass);
        String keyValidationType = keySerializer.getComparatorType().getTypeName();

        String comparatorTypesAlias = comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta,
                true);

        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, tableName,
                ComparatorType.COMPOSITETYPE);

        cfDef.setKeyValidationClass(keyValidationType);
        cfDef.setComparatorTypeAlias(comparatorTypesAlias);

        cfDef.setDefaultValidationClass(defaultValidationType);
        cfDef.setComment("Column family for clustered entity '" + entityName + "'");

        StringBuilder builder = new StringBuilder("\n\n");
        builder.append("Create column family for clustered entity '");
        builder.append(entityName).append("' : \n");
        builder.append("\tcreate column family ").append(tableName).append("\n");
        builder.append("\t\twith key_validation_class = ").append(keyValidationType).append("\n");
        builder.append("\t\tand comparator = '").append(ComparatorType.COMPOSITETYPE.getTypeName());
        builder.append(comparatorTypesAlias).append("'\n");
        builder.append("\t\tand default_validation_class = ").append(defaultValidationType).append("\n");
        builder.append("\t\tand comment = 'Column family for clustered entity").append(entityName).append("'\n\n");

        log.debug(builder.toString());

        return cfDef;
    }

    public ColumnFamilyDefinition createCounterCF(String keyspaceName) {
        ColumnFamilyDefinition counterCfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
                AchillesCounter.THRIFT_COUNTER_CF, COMPOSITETYPE);

        counterCfDef.setKeyValidationClass(COMPOSITETYPE.getTypeName());
        counterCfDef.setKeyValidationAlias(COUNTER_KEY_ALIAS);
        counterCfDef.setDefaultValidationClass(COUNTERTYPE.getClassName());
        counterCfDef.setComparatorTypeAlias(COUNTER_COMPARATOR_TYPE_ALIAS);

        counterCfDef.setComment("Generic Counter Column Family for Achilles");

        StringBuilder builder = new StringBuilder("\n\n");
        builder.append("Create generic counter column family for Achilles : \n");
        builder.append("\tcreate column family ").append(AchillesCounter.THRIFT_COUNTER_CF).append("\n");
        builder.append("\t\twith key_validation_class = '").append(COUNTER_KEY_CHECK).append("'\n");
        builder.append("\t\tand comparator = '").append(COUNTER_COMPARATOR_CHECK).append("'\n");
        builder.append("\t\tand default_validation_class = ").append(COUNTERTYPE.getTypeName()).append("\n");
        builder.append("\t\tand comment = 'Generic Counter Column Family for Achilles'\n\n");

        log.debug(builder.toString());

        return counterCfDef;

    }
}
