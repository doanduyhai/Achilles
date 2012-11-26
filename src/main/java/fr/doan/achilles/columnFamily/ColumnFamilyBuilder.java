package fr.doan.achilles.columnFamily;

import static fr.doan.achilles.validation.Validator.validateNotBlank;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.serializer.Utils;

public class ColumnFamilyBuilder
{

	public <ID> ColumnFamilyDefinition build(EntityMeta<ID> entityMeta, String keyspaceName)
	{
		validateNotBlank(keyspaceName, "keyspaceName");
		List<ColumnDefinition> columnMetadatas = new ArrayList<ColumnDefinition>();

		for (Entry<String, PropertyMeta<?>> entry : entityMeta.getPropertyMetas().entrySet())
		{
			BasicColumnDefinition column = new BasicColumnDefinition();
			column.setName(Utils.STRING_SRZ.toByteBuffer(entry.getKey()));
			column.setValidationClass(entry.getValue().getValueSerializer().getComparatorType().getClassName());
			columnMetadatas.add(column);
			// TODO fix metadatas
		}

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, entityMeta.getColumnFamilyName(),
				ComparatorType.COMPOSITETYPE);
		cfDef.setKeyValidationClass(entityMeta.getIdSerializer().getComparatorType().getTypeName());
		cfDef.setComparatorTypeAlias("(BytesType, UTF8Type, Int32Type)");
		cfDef.setComment("Column family for entity '" + entityMeta.getCanonicalClassName() + "'");
		return cfDef;
	}
}
