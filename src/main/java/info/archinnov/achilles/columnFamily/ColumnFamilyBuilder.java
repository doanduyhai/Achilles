package info.archinnov.achilles.columnFamily;

import static info.archinnov.achilles.serializer.SerializerUtils.OBJECT_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.InvalidColumnFamilyException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ColumnFamilyBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ColumnFamilyBuilder
{

	private static final Logger log = LoggerFactory.getLogger(ColumnFamilyBuilder.class);

	private static final String DYNAMIC_TYPE_ALIASES = "(a=>AsciiType,b=>BytesType,c=>BooleanType,d=>DateType,e=>DecimalType,z=>DoubleType,f=>FloatType,i=>IntegerType,j=>Int32Type,x=>LexicalUUIDType,l=>LongType,t=>TimeUUIDType,s=>UTF8Type,u=>UUIDType)";
	public static final Pattern CF_PATTERN = Pattern.compile("[a-zA-Z0-9_]{1,48}");
	public PropertyHelper helper = new PropertyHelper();

	public <ID> ColumnFamilyDefinition buildDynamicCompositeCF(EntityMeta<ID> entityMeta,
			String keyspaceName)
	{

		String entityName = entityMeta.getClassName();
		String columnFamilyName = entityMeta.getColumnFamilyName();

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				columnFamilyName, ComparatorType.DYNAMICCOMPOSITETYPE);

		String keyValidationType = entityMeta.getIdSerializer().getComparatorType().getTypeName();

		cfDef.setKeyValidationClass(keyValidationType);
		cfDef.setComparatorTypeAlias(DYNAMIC_TYPE_ALIASES);
		cfDef.setDefaultValidationClass(STRING_SRZ.getComparatorType().getTypeName());
		cfDef.setComment("Column family for entity '" + entityName + "'");

		StringBuilder builder = new StringBuilder("\n\n");
		builder.append("Create Dynamic Composite-based column family for entity ");
		builder.append("'").append(entityName).append("' : \n");
		builder.append("\tcreate column family ").append(columnFamilyName).append("\n");
		builder.append("\t\twith key_validation_class = ").append(keyValidationType).append("\n");
		builder.append("\t\tand comparator = '").append(
				ComparatorType.DYNAMICCOMPOSITETYPE.getTypeName());
		builder.append(DYNAMIC_TYPE_ALIASES).append("'\n");
		builder.append("\t\tand default_validation_class = ")
				.append(ComparatorType.UTF8TYPE.getTypeName()).append("\n");
		builder.append("\t\tand comment = 'Column family for entity ").append(entityName)
				.append("'\n\n");

		log.debug(builder.toString());

		return cfDef;
	}

	public <ID> ColumnFamilyDefinition buildCompositeCF(String keyspaceName,
			PropertyMeta<?, ?> propertyMeta, Class<ID> keyClass, String columnFamilyName,
			String entityName)
	{
		Class<?> valueClass = propertyMeta.getValueClass();

		Serializer<?> keySerializer = SerializerTypeInferer.getSerializer(keyClass);
		ComparatorType comparatorType = ComparatorType.COMPOSITETYPE;
		String comparatorTypesAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, true);

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				columnFamilyName, comparatorType);

		String keyValidationType = keySerializer.getComparatorType().getTypeName();
		cfDef.setKeyValidationClass(keyValidationType);
		cfDef.setComparatorTypeAlias(comparatorTypesAlias);

		Serializer<?> valueSerializer = SerializerTypeInferer.getSerializer(valueClass);

		if (valueSerializer == OBJECT_SRZ)
		{
			if (propertyMeta.type().isJoinColumn())
			{
				valueSerializer = propertyMeta.getJoinProperties().getEntityMeta().getIdMeta()
						.getValueSerializer();
			}
			else
			{
				valueSerializer = STRING_SRZ;
			}
		}

		String defaultValidationType = valueSerializer.getComparatorType().getTypeName();
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
			throw new InvalidColumnFamilyException(
					"The column family name '"
							+ cfName
							+ "' is invalid. It should be respect the pattern [a-zA-Z0-9_] and be at most 48 characters long");
		}
	}
}
