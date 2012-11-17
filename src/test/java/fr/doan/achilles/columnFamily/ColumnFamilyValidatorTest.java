package fr.doan.achilles.columnFamily;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.Test;

import fr.doan.achilles.exception.InvalidColumnFamilyException;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.SimplePropertyMeta;
import fr.doan.achilles.serializer.Utils;

public class ColumnFamilyValidatorTest
{

	private ColumnFamilyValidator columnFamilyValidator = new ColumnFamilyValidator();

	@Test
	public void should_validate_column_family() throws Exception
	{
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(null, "testCf", ComparatorType.COMPOSITETYPE);
		cfDef.setKeyValidationClass(Utils.LONG_SRZ.getComparatorType().getTypeName());

		Map<String, PropertyMeta<?>> map = new HashMap<String, PropertyMeta<?>>();

		SimplePropertyMeta<String> simplePropertyMeta = new SimplePropertyMeta<String>("name", String.class);
		map.put("name", simplePropertyMeta);
		EntityMeta<Long> meta = new EntityMeta<Long>(Long.class, "test.MyClass", 1L, map);

		columnFamilyValidator.validate(cfDef, meta);
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_not_matching_keyClass() throws Exception
	{
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(null, "testCf", ComparatorType.COMPOSITETYPE);
		cfDef.setKeyValidationClass(Utils.INT_SRZ.getComparatorType().getTypeName());

		Map<String, PropertyMeta<?>> map = new HashMap<String, PropertyMeta<?>>();

		SimplePropertyMeta<String> simplePropertyMeta = new SimplePropertyMeta<String>("name", String.class);
		map.put("name", simplePropertyMeta);
		EntityMeta<Long> meta = new EntityMeta<Long>(Long.class, "test.MyClass", 1L, map);

		columnFamilyValidator.validate(cfDef, meta);
	}

	@Test(expected = InvalidColumnFamilyException.class)
	public void should_exception_not_matching_column_comparator() throws Exception
	{
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(null, "testCf", ComparatorType.BYTESTYPE);
		cfDef.setKeyValidationClass(Utils.LONG_SRZ.getComparatorType().getTypeName());

		Map<String, PropertyMeta<?>> map = new HashMap<String, PropertyMeta<?>>();

		SimplePropertyMeta<String> simplePropertyMeta = new SimplePropertyMeta<String>("name", String.class);
		map.put("name", simplePropertyMeta);
		EntityMeta<Long> meta = new EntityMeta<Long>(Long.class, "test.MyClass", 1L, map);

		columnFamilyValidator.validate(cfDef, meta);
	}
}
