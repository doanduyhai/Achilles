package info.archinnov.achilles.columnFamily;

import static info.archinnov.achilles.dao.CounterDao.COUNTER_CF;
import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static me.prettyprint.hector.api.ddl.ComparatorType.*;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesInvalidColumnFamilyException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ColumnFamilyBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftColumnFamilyHelper
{
	private static final Logger log = LoggerFactory.getLogger(ThriftColumnFamilyHelper.class);

	public static final String ENTITY_TYPE_ALIAS = "(BytesType,UTF8Type,Int32Type)";
	public static final String SIMPLE_COUNTER_TYPE_ALIAS = "(UTF8Type)";
	public static final String COUNTER_KEY_ALIAS = "(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)";

	public static final Pattern CF_PATTERN = Pattern.compile("[a-zA-Z0-9_]{1,48}");
	public PropertyHelper helper = new PropertyHelper();

	public <ID> ColumnFamilyDefinition buildEntityCF(EntityMeta<ID> entityMeta, String keyspaceName)
	{

		String entityName = entityMeta.getClassName();
		String columnFamilyName = entityMeta.getColumnFamilyName();

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				columnFamilyName, ComparatorType.COMPOSITETYPE);

		String keyValidationType = entityMeta.getIdSerializer().getComparatorType().getTypeName();

		cfDef.setKeyValidationClass(keyValidationType);
		cfDef.setComparatorTypeAlias(ENTITY_TYPE_ALIAS);
		cfDef.setDefaultValidationClass(STRING_SRZ.getComparatorType().getTypeName());
		cfDef.setComment("Column family for entity '" + entityName + "'");

		StringBuilder builder = new StringBuilder("\n\n");
		builder.append("Create Composite-based column family for entity ");
		builder.append("'").append(entityName).append("' : \n");
		builder.append("\tcreate column family ").append(columnFamilyName).append("\n");
		builder.append("\t\twith key_validation_class = ").append(keyValidationType).append("\n");
		builder.append("\t\tand comparator = 'CompositeType").append(ENTITY_TYPE_ALIAS)
				.append("'\n");
		builder.append("\t\tand default_validation_class = ")
				.append(ComparatorType.UTF8TYPE.getTypeName()).append("\n");
		builder.append("\t\tand comment = 'Column family for entity ").append(entityName)
				.append("'\n\n");

		log.debug(builder.toString());

		return cfDef;
	}

	public <ID> ColumnFamilyDefinition buildDirectMappingCF(String keyspaceName,
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
					valueSerializer = propertyMeta.joinIdMeta().getValueSerializer();
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
		builder.append("Create Composite-based column family for property ");
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
		counterCfDef.setComparatorTypeAlias(SIMPLE_COUNTER_TYPE_ALIAS);

		counterCfDef.setComment("Generic Counter Column Family for Achilles");

		StringBuilder builder = new StringBuilder("\n\n");
		builder.append("Create generic counter column family for Achilles : \n");
		builder.append("\tcreate column family ").append(COUNTER_CF).append("\n");
		builder.append("\t\twith key_validation_class = ").append(COMPOSITETYPE.getTypeName());
		builder.append(COUNTER_KEY_ALIAS).append("\n");
		builder.append("\t\tand comparator = 'CompositeType").append(SIMPLE_COUNTER_TYPE_ALIAS)
				.append("'\n");
		builder.append("\t\tand default_validation_class = ").append(COUNTERTYPE.getTypeName())
				.append("\n");
		builder.append("\t\tand comment = 'Generic Counter Column Family for Achilles'\n\n");

		log.debug(builder.toString());

		return counterCfDef;

	}

	public void validateCFWithEntityMeta(ColumnFamilyDefinition cfDef, EntityMeta<?> entityMeta)
	{
		log.trace("Validating column family row key definition for entityMeta {}",
				entityMeta.getClassName());

		if (!StringUtils.equals(cfDef.getKeyValidationClass(), entityMeta.getIdSerializer()
				.getComparatorType().getClassName()))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '"
					+ entityMeta.getColumnFamilyName() + "' key class '"
					+ cfDef.getKeyValidationClass()
					+ "' does not correspond to the entity id class '"
					+ entityMeta.getIdSerializer().getComparatorType().getClassName() + "'");
		}

		if (entityMeta.isColumnFamilyDirectMapping())
		{
			this.validateCFWithPropertyMeta(cfDef, entityMeta.getPropertyMetas().values()
					.iterator().next(), entityMeta.getColumnFamilyName());
		}
		else
		{

			log.trace(
					"Validating column family  composite comparator definition for entityMeta {}",
					entityMeta.getClassName());

			if (cfDef.getComparatorType() == null
					|| !StringUtils.equals(cfDef.getComparatorType().getTypeName(),
							COMPOSITETYPE.getTypeName()))
			{
				throw new AchillesInvalidColumnFamilyException("The column family '"
						+ entityMeta.getColumnFamilyName() + "' comparator type should be '"
						+ COMPOSITETYPE.getTypeName() + "'");
			}

			if (!StringUtils.equals(cfDef.getComparatorTypeAlias(), ENTITY_TYPE_ALIAS))
			{
				throw new AchillesInvalidColumnFamilyException("The column family '"
						+ entityMeta.getColumnFamilyName() + "' comparator type alias should be '"
						+ ENTITY_TYPE_ALIAS + "'");
			}
		}
	}

	public void validateCFWithPropertyMeta(ColumnFamilyDefinition cfDef,
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

		if (!StringUtils.equals(cfDef.getKeyValidationClass(), COMPOSITETYPE.getClassName()))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '" + COUNTER_CF
					+ "' key class '" + cfDef.getKeyValidationClass() + "' should be '"
					+ COMPOSITETYPE.getClassName() + "'");
		}

		if (!StringUtils.equals(cfDef.getKeyValidationAlias(), COUNTER_KEY_ALIAS))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '" + COUNTER_CF
					+ "' key type alias '" + cfDef.getKeyValidationAlias() + "' should be '"
					+ COUNTER_KEY_ALIAS + "'");
		}

		if (cfDef.getComparatorType() != COMPOSITETYPE)
		{
			throw new AchillesInvalidColumnFamilyException("The column family '" + COUNTER_CF
					+ "' comparator type '" + cfDef.getComparatorType().getTypeName()
					+ "' should be '" + COMPOSITETYPE.getTypeName() + "'");
		}

		if (!StringUtils.equals(cfDef.getComparatorTypeAlias(), SIMPLE_COUNTER_TYPE_ALIAS))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '" + COUNTER_CF
					+ "' comparator type alias '" + cfDef.getComparatorTypeAlias()
					+ "' should be '" + SIMPLE_COUNTER_TYPE_ALIAS + "'");
		}

		if (!StringUtils.equals(cfDef.getDefaultValidationClass(), COUNTERTYPE.getClassName()))
		{
			throw new AchillesInvalidColumnFamilyException("The column family '" + COUNTER_CF
					+ "' validation class '" + cfDef.getDefaultValidationClass() + "' should be '"
					+ COUNTERTYPE.getClassName() + "'");
		}
	}

	public static String normalizerAndValidateColumnFamilyName(String cfName)
	{
		log.trace("Normalizing column family '{}' name agains Cassandra restrictions", cfName);

		Matcher nameMatcher = CF_PATTERN.matcher(cfName);

		if (nameMatcher.matches())
		{
			return cfName;
		}
		else if (cfName.contains("."))
		{
			String className = cfName.replaceAll(".+\\.(.+)", "$1");
			return normalizerAndValidateColumnFamilyName(className);
		}
		else
		{
			throw new AchillesInvalidColumnFamilyException(
					"The column family name '"
							+ cfName
							+ "' is invalid. It should be respect the pattern [a-zA-Z0-9_] and be at most 48 characters long");
		}
	}
}
