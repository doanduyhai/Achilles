package info.archinnov.achilles.table;

import static me.prettyprint.hector.api.ddl.ComparatorType.COUNTERTYPE;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import info.archinnov.achilles.validation.Validator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftColumnFamilyValidator {
    protected static final Logger log = LoggerFactory.getLogger(ThriftColumnFamilyValidator.class);
    public static final String ENTITY_COMPARATOR_TYPE_CHECK = "CompositeType(org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";
    public static final String COUNTER_KEY_CHECK = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)";
    public static final String COUNTER_COMPARATOR_CHECK = "CompositeType(org.apache.cassandra.db.marshal.UTF8Type)";

    private ThriftComparatorTypeAliasFactory comparatorAliasFactory = new ThriftComparatorTypeAliasFactory();

    public void validateCFForEntity(ColumnFamilyDefinition cfDef, EntityMeta entityMeta) {
        log.trace("Validating column family row key definition for entityMeta {}", entityMeta.getClassName());

        Serializer<?> idSerializer = ThriftSerializerTypeInferer.getSerializer(entityMeta.getIdClass());

        Validator.validateTableTrue(
                StringUtils.equals(cfDef.getKeyValidationClass(), idSerializer.getComparatorType().getClassName()),
                "The column family '" + entityMeta.getTableName()
                        + "' key class '" + cfDef.getKeyValidationClass()
                        + "' does not correspond to the entity id class '"
                        + idSerializer.getComparatorType().getClassName() + "'");

        log.trace("Validating column family  composite comparator definition for entityMeta {}",
                entityMeta.getClassName());

        String comparatorType = (cfDef.getComparatorType() != null ? cfDef.getComparatorType().getTypeName() : "")
                + cfDef.getComparatorTypeAlias();

        Validator.validateTableTrue(StringUtils.equals(comparatorType, ENTITY_COMPARATOR_TYPE_CHECK),
                "The column family '" + entityMeta.getTableName() + "' comparator type '" + comparatorType
                        + "' should be '" + ENTITY_COMPARATOR_TYPE_CHECK + "'");

    }

    public void validateWideRowForProperty(ColumnFamilyDefinition cfDef, PropertyMeta<?, ?> propertyMeta,
            String tableName) {
        log.trace(
                "Validating column family composite comparator definition for propertyMeta {} and column family {}",
                propertyMeta.getPropertyName(), tableName);

        String comparatorTypeAlias = comparatorAliasFactory.determineCompatatorTypeAliasForCompositeCF(propertyMeta,
                false);

        String comparatorType = (cfDef.getComparatorType() != null ? cfDef.getComparatorType().getTypeName() : "")
                + cfDef.getComparatorTypeAlias();

        Validator.validateTableTrue(StringUtils.equals(comparatorType, comparatorTypeAlias),
                "The column family '" + tableName + "' comparator type should be '" + comparatorTypeAlias + "' : "
                        + comparatorType);
    }

    public void validateWideRowForClusteredEntity(ColumnFamilyDefinition cfDef, EntityMeta meta, String tableName) {

        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        PropertyMeta<?, ?> pm = meta.getFirstMeta();

        log.trace(
                "Validating column family composite comparator definition for clustered entity {} and column family {}",
                idMeta.getEntityClassName(), tableName);

        Class<?> keyClass = idMeta.getComponentClasses().get(0);
        String keyValidationType = ThriftSerializerTypeInferer.<Object> getSerializer(keyClass).getComparatorType()
                .getClassName();

        Validator.validateTableTrue(StringUtils.equals(cfDef.getKeyValidationClass(), keyValidationType),
                "The column family '" + tableName + "' key validation type should be '" + keyValidationType + "'");

        String comparatorTypeAlias = comparatorAliasFactory.determineCompatatorTypeAliasForClusteredEntity(idMeta,
                false);

        String comparatorType = (cfDef.getComparatorType() != null ? cfDef.getComparatorType().getTypeName() : "")
                + cfDef.getComparatorTypeAlias();

        Validator.validateTableTrue(StringUtils.equals(comparatorType, comparatorTypeAlias),
                "The column family '" + tableName + "' comparator type should be '" + comparatorTypeAlias + "'");

        String valueValidationType;
        PropertyType type = pm.type();
        if (type.isCounter()) {
            valueValidationType = COUNTERTYPE.getClassName();
        } else if (type.isJoin()) {
            valueValidationType = ThriftSerializerTypeInferer.getSerializer(pm.joinIdMeta().getValueClass())
                    .getComparatorType().getTypeName();
        } else {
            valueValidationType = ThriftSerializerTypeInferer.getSerializer(pm.getValueClass()).getComparatorType()
                    .getTypeName();
        }

        Validator.validateTableTrue(StringUtils.equals(cfDef.getDefaultValidationClass(), valueValidationType),
                "The column family '" + tableName + "' default validation type should be '" + valueValidationType
                        + "'");

    }

    public void validateCounterCF(ColumnFamilyDefinition cfDef) {
        log.trace("Validating counter column family row key definition ");

        String keyValidation = cfDef.getKeyValidationClass() + cfDef.getKeyValidationAlias();

        Validator.validateTableTrue(StringUtils.equals(keyValidation, COUNTER_KEY_CHECK),
                "The column family '" + AchillesCounter.THRIFT_COUNTER_CF + "' key class '" + keyValidation
                        + "' should be '" + COUNTER_KEY_CHECK + "'");

        String comparatorType = (cfDef.getComparatorType() != null ? cfDef.getComparatorType().getTypeName() : "")
                + cfDef.getComparatorTypeAlias();

        Validator.validateTableTrue(StringUtils.equals(comparatorType, COUNTER_COMPARATOR_CHECK),
                "The column family '" + AchillesCounter.THRIFT_COUNTER_CF
                        + "' comparator type '" + comparatorType + "' should be '" + COUNTER_COMPARATOR_CHECK + "'");

        Validator.validateTableTrue(
                StringUtils.equals(cfDef.getDefaultValidationClass(), COUNTERTYPE.getClassName()),
                "The column family '" + AchillesCounter.THRIFT_COUNTER_CF + "' validation class '"
                        + cfDef.getDefaultValidationClass() + "' should be '" + COUNTERTYPE.getClassName() + "'");

    }

}
