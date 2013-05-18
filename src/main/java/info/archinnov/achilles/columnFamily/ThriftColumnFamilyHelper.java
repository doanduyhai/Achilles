package info.archinnov.achilles.columnFamily;

import static info.archinnov.achilles.dao.ThriftCounterDao.COUNTER_CF;
import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static me.prettyprint.hector.api.ddl.ComparatorType.*;
import info.archinnov.achilles.entity.ThriftPropertyHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesInvalidColumnFamilyException;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.commons.lang.StringUtils;

/**
 * ColumnFamilyBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftColumnFamilyHelper extends AchillesColumnFamilyHelper
{
	public static final String ENTITY_COMPARATOR_TYPE_ALIAS = "(org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";
	public static final String ENTITY_COMPARATOR_TYPE_CHECK = "CompositeType(org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";

	public static final String COUNTER_KEY_ALIAS = "(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)";
	public static final String COUNTER_KEY_CHECK = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)";
	public static final String COUNTER_COMPARATOR_TYPE_ALIAS = "(org.apache.cassandra.db.marshal.UTF8Type)";
	public static final String COUNTER_COMPARATOR_CHECK = "CompositeType(org.apache.cassandra.db.marshal.UTF8Type)";

	public ThriftPropertyHelper helper = new ThriftPropertyHelper();

	public ColumnFamilyDefinition buildEntityCF(EntityMeta entityMeta, String keyspaceName)
	{

		String entityName = entityMeta.getClassName();
		String columnFamilyName = entityMeta.getColumnFamilyName();

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				columnFamilyName, ComparatorType.COMPOSITETYPE);

		Serializer<?> idSerializer = SerializerTypeInferer.getSerializer(entityMeta.getIdClass());
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
		builder.append("\t\tand default_validation_class = ")
				.append(ComparatorType.UTF8TYPE.getTypeName()).append("\n");
		builder.append("\t\tand comment = 'Column family for entity ").append(entityName)
				.append("'\n\n");

		log.debug(builder.toString());

		return cfDef;
	}

	public <ID> ColumnFamilyDefinition buildWideRowCF(String keyspaceName,
			PropertyMeta<?, ?> propertyMeta, Class<ID> keyClass, String columnFamilyName,
			String entityName)
	{
		Class<?> valueClass = propertyMeta.getValueClass();

		Serializer<?> keySerializer = SerializerTypeInferer.getSerializer(keyClass);;
		String keyValidationType = keySerializer.getComparatorType().getTypeName();;

		String comparatorTypesAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, true);

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				columnFamilyName, ComparatorType.COMPOSITETYPE);

		cfDef.setKeyValidationClass(keyValidationType);
		cfDef.setComparatorTypeAlias(comparatorTypesAlias);

		Serializer<?> valueSerializer;
		String defaultValidationType;
		if (propertyMeta.isCounter())
		{
			valueSerializer = LONG_SRZ;
			defaultValidationType = COUNTERTYPE.getTypeName();
		}
		else
		{
			valueSerializer = SerializerTypeInferer.getSerializer(valueClass);
			if (valueSerializer == OBJECT_SRZ)
			{
				if (propertyMeta.isJoin())
				{
					valueSerializer = SerializerTypeInferer.getSerializer(propertyMeta.joinIdMeta()
							.getValueClass());
				}
				else
				{
					valueSerializer = STRING_SRZ;
				}
			}
			defaultValidationType = valueSerializer.getComparatorType().getTypeName();
		}

		cfDef.setDefaultValidationClass(defaultValidationType);
		cfDef.setComment("Column family for entity '" + columnFamilyName + "'");

		String propertyName = propertyMeta.getPropertyName();

		StringBuilder builder = new StringBuilder("\n\n");
		builder.append("Create wide row column family for property ");
		builder.append("'").append(propertyName).append("' of entity '");
		builder.append(entityName).append("' : \n");
		builder.append("\tcreate column family ").append(columnFamilyName).append("\n");
		builder.append("\t\twith key_validation_class = ").append(keyValidationType).append("\n");
		builder.append("\t\tand comparator = '").append(ComparatorType.COMPOSITETYPE.getTypeName());
		builder.append(comparatorTypesAlias).append("'\n");
		builder.append("\t\tand default_validation_class = ").append(defaultValidationType)
				.append("\n");
		builder.append("\t\tand comment = 'Column family for property ").append(propertyName);
		builder.append(" of entity ").append(entityName).append("'\n\n");

		log.debug(builder.toString());

		return cfDef;
	}

	public ColumnFamilyDefinition buildCounterCF(String keyspaceName)
	{
		ColumnFamilyDefinition counterCfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				COUNTER_CF, COMPOSITETYPE);

		counterCfDef.setKeyValidationClass(COMPOSITETYPE.getTypeName());
		counterCfDef.setKeyValidationAlias(COUNTER_KEY_ALIAS);
		counterCfDef.setDefaultValidationClass(COUNTERTYPE.getClassName());
		counterCfDef.setComparatorTypeAlias(COUNTER_COMPARATOR_TYPE_ALIAS);

		counterCfDef.setComment("Generic Counter Column Family for Achilles");

		StringBuilder builder = new StringBuilder("\n\n");
		builder.append("Create generic counter column family for Achilles : \n");
		builder.append("\tcreate column family ").append(COUNTER_CF).append("\n");
		builder.append("\t\twith key_validation_class = '").append(COUNTER_KEY_CHECK).append("'\n");
		builder.append("\t\tand comparator = '").append(COUNTER_COMPARATOR_CHECK).append("'\n");
		builder.append("\t\tand default_validation_class = ").append(COUNTERTYPE.getTypeName())
				.append("\n");
		builder.append("\t\tand comment = 'Generic Counter Column Family for Achilles'\n\n");

		log.debug(builder.toString());

		return counterCfDef;

	}

	public void validateCFWithEntityMeta(ColumnFamilyDefinition cfDef, EntityMeta entityMeta)
	{
		log.trace("Validating column family row key definition for entityMeta {}",
				entityMeta.getClassName());

		Serializer<?> idSerializer = SerializerTypeInferer.getSerializer(entityMeta.getIdClass());

		if (!StringUtils.equals(cfDef.getKeyValidationClass(), idSerializer.getComparatorType()
				.getClassName()))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '"
					+ entityMeta.getColumnFamilyName() + "' key class '"
					+ cfDef.getKeyValidationClass()
					+ "' does not correspond to the entity id class '"
					+ idSerializer.getComparatorType().getClassName() + "'");
		}

		if (entityMeta.isWideRow())
		{
			this.validateWideRowWithPropertyMeta(cfDef, entityMeta.getPropertyMetas().values()
					.iterator().next(), entityMeta.getColumnFamilyName());
		}
		else
		{

			log.trace(
					"Validating column family  composite comparator definition for entityMeta {}",
					entityMeta.getClassName());

			String comparatorType = (cfDef.getComparatorType() != null ? cfDef.getComparatorType()
					.getTypeName() : "") + cfDef.getComparatorTypeAlias();

			if (!StringUtils.equals(comparatorType, ENTITY_COMPARATOR_TYPE_CHECK))
			{
				throw new AchillesInvalidColumnFamilyException("The column family '"
						+ entityMeta.getColumnFamilyName() + "' comparator type '" + comparatorType
						+ "' should be '" + ENTITY_COMPARATOR_TYPE_CHECK + "'");
			}
		}
	}

	public void validateWideRowWithPropertyMeta(ColumnFamilyDefinition cfDef,
			PropertyMeta<?, ?> propertyMeta, String externalColumnFamilyName)
	{
		log.trace(
				"Validating column family composite comparator definition for propertyMeta {} and column family {}",
				propertyMeta.getPropertyName(), externalColumnFamilyName);

		String comparatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, false);
		if (cfDef.getComparatorType() == null
				|| !StringUtils
						.equals(cfDef.getComparatorType().getTypeName(), comparatorTypeAlias))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '"
					+ externalColumnFamilyName + "' comparator type should be '"
					+ comparatorTypeAlias + "'");
		}
	}

	public void validateCounterCF(ColumnFamilyDefinition cfDef)
	{
		log.trace("Validating counter column family row key definition ");

		String keyValidation = cfDef.getKeyValidationClass() + cfDef.getKeyValidationAlias();
		if (!StringUtils.equals(keyValidation, COUNTER_KEY_CHECK))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '" + COUNTER_CF
					+ "' key class '" + keyValidation + "' should be '" + COUNTER_KEY_CHECK + "'");
		}

		String comparatorType = (cfDef.getComparatorType() != null ? cfDef.getComparatorType()
				.getTypeName() : "") + cfDef.getComparatorTypeAlias();
		if (!StringUtils.equals(comparatorType, COUNTER_COMPARATOR_CHECK))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '" + COUNTER_CF
					+ "' comparator type '" + comparatorType + "' should be '"
					+ COUNTER_COMPARATOR_CHECK + "'");
		}

		if (!StringUtils.equals(cfDef.getDefaultValidationClass(), COUNTERTYPE.getClassName()))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '" + COUNTER_CF
					+ "' validation class '" + cfDef.getDefaultValidationClass() + "' should be '"
					+ COUNTERTYPE.getClassName() + "'");
		}
	}
}
