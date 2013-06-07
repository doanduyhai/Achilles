package info.archinnov.achilles.entity.parsing.validator;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import testBuilders.PropertyMetaTestBuilder;

/**
 * AchillesEntityParsingValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParsingValidatorTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private EntityParsingValidator validator = new EntityParsingValidator();

	@Test
	public void should_exception_when_no_id_meta() throws Exception
	{
		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The entity '" + CompleteBean.class.getCanonicalName()
				+ "' should have at least one field with javax.persistence.Id annotation");
		validator.validateHasIdMeta(CompleteBean.class, null);
	}

	@Test
	public void should_exception_when_empty_property_meta_map() throws Exception
	{
		EntityParsingContext context = new EntityParsingContext(null, null,
				CompleteBean.class);
		context.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());
		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("The entity '"
						+ CompleteBean.class.getCanonicalName()
						+ "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");

		validator.validatePropertyMetas(context);
	}

	@Test
	public void should_exception_when_more_than_one_property_meta_for_wide_row() throws Exception
	{
		EntityParsingContext context = new EntityParsingContext(null, null,
				CompleteBean.class);
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetas.put("name", null);
		propertyMetas.put("age", null);
		context.setPropertyMetas(propertyMetas);
		context.setWideRow(true);

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The ColumnFamily entity '" + CompleteBean.class.getCanonicalName()
				+ "' should not have more than one property annotated with @Column");

		validator.validateWideRows(context);
	}

	@Test
	public void should_exception_when_incorrect_type_of_property_meta_for_wide_row()
			throws Exception
	{
		EntityParsingContext context = new EntityParsingContext(null, null,
				CompleteBean.class);
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
				.valueClass(String.class)
				// /
				.type(PropertyType.SIMPLE)
				//
				.build();
		propertyMetas.put("name", propertyMeta);
		context.setPropertyMetas(propertyMetas);
		context.setWideRow(true);

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The ColumnFamily entity '" + CompleteBean.class.getCanonicalName()
				+ "' should have one and only one @Column/@JoinColumn of type WideMap");

		validator.validateWideRows(context);
	}

	@Test
	public void should_exception_when_join_entity_is_wide_row() throws Exception
	{
		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
				.valueClass(String.class)
				//
				.field("test")
				//
				.entityClassName("entity")
				//
				.build();

		EntityMeta joinMeta = new EntityMeta();
		joinMeta.setWideRow(true);
		joinMeta.setClassName("class.name");
		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("The entity 'class.name' is a Wide row and cannot be a join entity");

		validator.validateJoinEntityNotWideRow(propertyMeta, joinMeta);
	}

	@Test
	public void should_exception_when_join_entity_does_not_exist_in_properties_map()
			throws Exception
	{
		Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
		entityMetaMap.put(this.getClass(), null);

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("Cannot find mapping for join entity '"
				+ CompleteBean.class.getCanonicalName() + "'");

		validator.validateJoinEntityExist(entityMetaMap, CompleteBean.class);

	}

	@Test
	public void should_exception_when_no_entity_found_after_parsing() throws Exception
	{
		List<Class<?>> entities = new ArrayList<Class<?>>();
		List<String> entityPackages = Arrays.asList("com.package1", "com.package2");

		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages 'com.package1,com.package2'");

		validator.validateAtLeastOneEntity(entities, entityPackages);

	}
}
