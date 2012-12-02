package fr.doan.achilles.columnFamily;

import static fr.doan.achilles.validation.Validator.validateNotBlank;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import fr.doan.achilles.entity.metadata.EntityMeta;

public class ColumnFamilyBuilder
{

	private static final String DYNAMIC_TYPE_ALIASES = "(a=>AsciiType,b=>BytesType,c=>BooleanType,d=>DateType,e=>DecimalType,z=>DoubleType,f=>FloatType,i=>IntegerType,j=>Int32Type,x=>LexicalUUIDType,l=>LongType,t=>TimeUUIDType,s=>UTF8Type,u=>UUIDType)";

	public <ID> ColumnFamilyDefinition build(EntityMeta<ID> entityMeta, String keyspaceName)
	{
		validateNotBlank(keyspaceName, "keyspaceName");

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, entityMeta.getColumnFamilyName(),
				ComparatorType.DYNAMICCOMPOSITETYPE);

		cfDef.setKeyValidationClass(entityMeta.getIdSerializer().getComparatorType().getTypeName());
		cfDef.setComparatorTypeAlias(DYNAMIC_TYPE_ALIASES);
		cfDef.setComment("Column family for entity '" + entityMeta.getCanonicalClassName() + "'");

		return cfDef;
	}
}
