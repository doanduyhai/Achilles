package fr.doan.achilles.parser;

import static fr.doan.achilles.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import me.prettyprint.hector.api.Keyspace;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.BeanWithColumnFamilyName;
import parser.entity.BeanWithNoColumn;
import parser.entity.BeanWithNoId;
import parser.entity.BeanWithNoSerialVersionUID;
import parser.entity.BeanWithNoTableAnnotation;
import parser.entity.BeanWithNonPublicSerialVersionUID;
import parser.entity.BeanWithNotSerializableId;
import parser.entity.CompleteBean;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.metadata.SetPropertyMeta;
import fr.doan.achilles.serializer.Utils;

@RunWith(MockitoJUnitRunner.class)
public class EntityParserTest
{
	private final EntityParser parser = new EntityParser();

	@Mock
	private Keyspace keyspace;

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_entity() throws Exception
	{
		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace, CompleteBean.class);

		assertThat(meta.getCanonicalClassName()).isEqualTo("parser.entity.CompleteBeanForParser");
		assertThat(meta.getColumnFamilyName()).isEqualTo("parser_entity_CompleteBeanForParser");
		assertThat(meta.getSerialVersionUID()).isEqualTo(1L);
		assertThat(meta.getIdMeta().getValueClass()).isEqualTo(Long.class);
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getIdMeta().getValueSerializer().getComparatorType()).isEqualTo(LONG_SRZ.getComparatorType());
		assertThat(meta.getIdSerializer()).isEqualTo(LONG_SRZ);
		assertThat(meta.getPropertyMetas()).hasSize(5);

		PropertyMeta<?> name = meta.getPropertyMetas().get("name");
		PropertyMeta<?> age = meta.getPropertyMetas().get("age_in_year");
		ListPropertyMeta<String> friends = (ListPropertyMeta<String>) meta.getPropertyMetas().get("friends");
		SetPropertyMeta<String> followers = (SetPropertyMeta<String>) meta.getPropertyMetas().get("followers");
		MapPropertyMeta<String> preferences = (MapPropertyMeta<String>) meta.getPropertyMetas().get("preferences");

		assertThat(name).isNotNull();
		assertThat(age).isNotNull();
		assertThat(friends).isNotNull();
		assertThat(followers).isNotNull();
		assertThat(preferences).isNotNull();

		assertThat(name.getPropertyName()).isEqualTo("name");
		assertThat(name.getValueClass()).isEqualTo(String.class);
		assertThat(name.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(name.propertyType()).isEqualTo(SIMPLE);

		assertThat(age.getPropertyName()).isEqualTo("age_in_year");
		assertThat(age.getValueClass()).isEqualTo(Long.class);
		assertThat(age.getValueSerializer()).isEqualTo(LONG_SRZ);
		assertThat(age.propertyType()).isEqualTo(SIMPLE);

		assertThat(friends.getPropertyName()).isEqualTo("friends");
		assertThat(friends.getValueClass()).isEqualTo(String.class);
		assertThat(friends.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(friends.propertyType()).isEqualTo(PropertyType.LIST);
		assertThat(friends.newListInstance()).isNotNull();
		assertThat(friends.newListInstance()).isEmpty();
		assertThat(friends.newListInstance().getClass()).isEqualTo(ArrayList.class);

		assertThat(followers.getPropertyName()).isEqualTo("followers");
		assertThat(followers.getValueClass()).isEqualTo(String.class);
		assertThat(followers.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(followers.propertyType()).isEqualTo(PropertyType.SET);
		assertThat(followers.newSetInstance()).isNotNull();
		assertThat(followers.newSetInstance()).isEmpty();
		assertThat(followers.newSetInstance().getClass()).isEqualTo(HashSet.class);

		assertThat(preferences.getPropertyName()).isEqualTo("preferences");
		assertThat(preferences.getValueClass()).isEqualTo(String.class);
		assertThat(preferences.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(preferences.propertyType()).isEqualTo(PropertyType.MAP);
		assertThat(preferences.getKeyClass()).isEqualTo(Integer.class);
		assertThat(preferences.getKeySerializer()).isEqualTo(Utils.INT_SRZ);
		assertThat(preferences.newMapInstance()).isNotNull();
		assertThat(preferences.newMapInstance()).isEmpty();
		assertThat(preferences.newMapInstance().getClass()).isEqualTo(HashMap.class);
	}

	@SuppressWarnings(
	{
		"unchecked"
	})
	@Test
	public void should_parse_entity_with_table_name() throws Exception
	{

		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace, BeanWithColumnFamilyName.class);

		assertThat(meta).isNotNull();
		assertThat(meta.getColumnFamilyName()).isEqualTo("myOwnCF");
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_table_annotation() throws Exception
	{
		parser.parseEntity(keyspace, BeanWithNoTableAnnotation.class);
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_serialVersionUID() throws Exception
	{
		parser.parseEntity(keyspace, BeanWithNoSerialVersionUID.class);
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_non_public_serialVersionUID() throws Exception
	{
		parser.parseEntity(keyspace, BeanWithNonPublicSerialVersionUID.class);
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_id() throws Exception
	{
		parser.parseEntity(keyspace, BeanWithNoId.class);
	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_id_type_not_serializable() throws Exception
	{

		parser.parseEntity(keyspace, BeanWithNotSerializableId.class);
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_column() throws Exception
	{

		parser.parseEntity(keyspace, BeanWithNoColumn.class);
	}

}
