package info.archinnov.achilles.columnFamily;

import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
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

	private PropertyHelper helper = new PropertyHelper();

	public <ID> ColumnFamilyDefinition buildDynamicCompositeCF(EntityMeta<ID> entityMeta,
			String keyspaceName)
	{

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				entityMeta.getColumnFamilyName(), ComparatorType.DYNAMICCOMPOSITETYPE);

		cfDef.setKeyValidationClass(entityMeta.getIdSerializer().getComparatorType().getTypeName());
		cfDef.setComparatorTypeAlias(DYNAMIC_TYPE_ALIASES);
		cfDef.setComment("Column family for entity '" + entityMeta.getClassName() + "'");

		log.debug("Create Dynamic Composite-based column family for entityMeta {}",
				entityMeta.getClassName());

		return cfDef;
	}

	public <ID> ColumnFamilyDefinition buildCompositeCF(String keyspaceName,
			PropertyMeta<?, ?> propertyMeta, Class<ID> keyClass, String columnFamilyName)
	{
		Class<?> valueClass = propertyMeta.getValueClass();

		Serializer<?> keySerializer = SerializerTypeInferer.getSerializer(keyClass);
		ComparatorType comparatorType = ComparatorType.COMPOSITETYPE;
		String comparatorTypesAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, true);

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				columnFamilyName, comparatorType);

		cfDef.setKeyValidationClass(keySerializer.getComparatorType().getTypeName());
		cfDef.setComparatorTypeAlias(comparatorTypesAlias);

		Serializer<?> valueSerializer = SerializerTypeInferer.getSerializer(valueClass);

		cfDef.setDefaultValidationClass(valueSerializer.getComparatorType().getTypeName());
		cfDef.setComment("Column family for entity '" + columnFamilyName + "'");

		log.debug("Create Composite-based column family for propertyMeta {}",
				propertyMeta.getPropertyName());

		return cfDef;
	}
}
