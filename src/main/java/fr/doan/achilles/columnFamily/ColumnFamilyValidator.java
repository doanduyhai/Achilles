package fr.doan.achilles.columnFamily;

import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.exception.InvalidColumnFamilyException;
import fr.doan.achilles.metadata.EntityMeta;

public class ColumnFamilyValidator
{

	private String COMPARATOR_TYPE_AND_ALIAS = "CompositeType(org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";

	public void validate(ColumnFamilyDefinition cfDef, EntityMeta<?> entityMeta)
	{

		if (!StringUtils.equals(cfDef.getKeyValidationClass(), entityMeta.getIdSerializer().getComparatorType().getClassName()))
		{
			throw new InvalidColumnFamilyException("The column family '" + entityMeta.getColumnFamilyName() + "' key class '"
					+ cfDef.getKeyValidationClass() + "' does not correspond to the entity id class '"
					+ entityMeta.getIdSerializer().getComparatorType().getClassName() + "'");
		}

		if (cfDef.getComparatorType() == null || !StringUtils.equals(cfDef.getComparatorType().getTypeName(), COMPARATOR_TYPE_AND_ALIAS))
		{
			throw new InvalidColumnFamilyException("The column family '" + entityMeta.getColumnFamilyName() + "' comparator type should be '"
					+ COMPARATOR_TYPE_AND_ALIAS);
		}
	}
}
