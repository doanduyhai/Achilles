package fr.doan.achilles.parser;

import static fr.doan.achilles.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import org.junit.Test;

import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.exception.NotSerializableException;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.metadata.SetPropertyMeta;
import fr.doan.achilles.serializer.Utils;

public class EntityParserTest
{
	private EntityParser parser = new EntityParser();

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_entity() throws Exception
	{
		EntityMeta<Long> meta = parser.parseEntity(TestEntity.class);

		assertThat(meta.getCanonicalClassName()).isEqualTo("fr.doan.achilles.parser.EntityParserTest.TestEntity");
		assertThat(meta.getColumnFamilyName()).isEqualTo("fr_doan_achilles_parser_EntityParserTest_TestEntity");
		assertThat(meta.getSerialVersionUID()).isEqualTo(1L);
		assertThat(meta.getIdClass()).isEqualTo(Long.class);
		assertThat(meta.getIdSerializer()).isEqualTo(LONG_SRZ);
		assertThat(meta.getAttributes()).hasSize(5);

		PropertyMeta<?> name = meta.getAttributes().get("name");
		PropertyMeta<?> age = meta.getAttributes().get("age_in_year");
		ListPropertyMeta<String> friends = (ListPropertyMeta<String>) meta.getAttributes().get("friends");
		SetPropertyMeta<String> followers = (SetPropertyMeta<String>) meta.getAttributes().get("followers");
		MapPropertyMeta<String> preferences = (MapPropertyMeta<String>) meta.getAttributes().get("preferences");

		assertThat(name).isNotNull();
		assertThat(age).isNotNull();
		assertThat(friends).isNotNull();
		assertThat(followers).isNotNull();
		assertThat(preferences).isNotNull();

		assertThat(name.getName()).isEqualTo("name");
		assertThat(name.getValueCanonicalClassName()).isEqualTo("java.lang.String");
		assertThat(name.getValueClass()).isEqualTo(String.class);
		assertThat(name.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(name.propertyType()).isEqualTo(SIMPLE);

		assertThat(age.getName()).isEqualTo("age_in_year");
		assertThat(age.getValueCanonicalClassName()).isEqualTo("java.lang.Long");
		assertThat(age.getValueClass()).isEqualTo(Long.class);
		assertThat(age.getValueSerializer()).isEqualTo(LONG_SRZ);
		assertThat(age.propertyType()).isEqualTo(SIMPLE);

		assertThat(friends.getName()).isEqualTo("friends");
		assertThat(friends.getValueCanonicalClassName()).isEqualTo("java.lang.String");
		assertThat(friends.getValueClass()).isEqualTo(String.class);
		assertThat(friends.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(friends.propertyType()).isEqualTo(PropertyType.LIST);
		assertThat(friends.newListInstance()).isNotNull();
		assertThat(friends.newListInstance()).isEmpty();
		assertThat(friends.newListInstance().getClass()).isEqualTo(ArrayList.class);

		assertThat(followers.getName()).isEqualTo("followers");
		assertThat(followers.getValueCanonicalClassName()).isEqualTo("java.lang.String");
		assertThat(followers.getValueClass()).isEqualTo(String.class);
		assertThat(followers.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(followers.propertyType()).isEqualTo(PropertyType.SET);
		assertThat(followers.newSetInstance()).isNotNull();
		assertThat(followers.newSetInstance()).isEmpty();
		assertThat(followers.newSetInstance().getClass()).isEqualTo(HashSet.class);

		assertThat(preferences.getName()).isEqualTo("preferences");
		assertThat(preferences.getValueCanonicalClassName()).isEqualTo("java.lang.String");
		assertThat(preferences.getValueClass()).isEqualTo(String.class);
		assertThat(preferences.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(preferences.propertyType()).isEqualTo(PropertyType.MAP);
		assertThat(preferences.getKeyClass()).isEqualTo(Integer.class);
		assertThat(preferences.getKeyClassSerializer()).isEqualTo(Utils.INT_SRZ);
		assertThat(preferences.newMapInstance()).isNotNull();
		assertThat(preferences.newMapInstance()).isEmpty();
		assertThat(preferences.newMapInstance().getClass()).isEqualTo(HashMap.class);
	}

	@Test
	public void should_parse_entity_with_table_name() throws Exception
	{
		@Table(name = "myOwnCF")
		class TestEntityWithColumnFamilyName implements Serializable
		{
			private static final long serialVersionUID = 1L;

			@Id
			private Long id;

			@Column
			private String name;
		}

		EntityMeta<Long> meta = parser.parseEntity(TestEntityWithColumnFamilyName.class);

		assertThat(meta).isNotNull();
		assertThat(meta.getColumnFamilyName()).isEqualTo("myOwnCF");
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_table_annotation() throws Exception
	{
		@SuppressWarnings("serial")
		class TestEntityNoTableAnnotation implements Serializable
		{}
		parser.parseEntity(TestEntityNoTableAnnotation.class);
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_serialVersionUID() throws Exception
	{
		@SuppressWarnings("serial")
		class TestEntityNoSerialVersionUID implements Serializable
		{}
		parser.parseEntity(TestEntityNoSerialVersionUID.class);
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_non_public_serialVersionUID() throws Exception
	{
		class TestEntityNonPublicSerialVersionUID implements Serializable
		{
			static final long serialVersionUID = 1L;
		}
		parser.parseEntity(TestEntityNonPublicSerialVersionUID.class);
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_id() throws Exception
	{
		@Table
		class TestEntityNoId implements Serializable
		{
			private static final long serialVersionUID = 1L;
		}
		parser.parseEntity(TestEntityNoId.class);
	}

	@Test(expected = NotSerializableException.class)
	public void should_exception_when_id_type_not_serializable() throws Exception
	{
		class NotSerializableId
		{}
		@Table
		class TestEntityNotSerializableId implements Serializable
		{
			private static final long serialVersionUID = 1L;

			@Id
			private NotSerializableId id;
		}

		parser.parseEntity(TestEntityNotSerializableId.class);
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_column() throws Exception
	{
		@Table
		class TestEntityNoColumn implements Serializable
		{
			private static final long serialVersionUID = 1L;

			@Id
			private Long id;
		}
		parser.parseEntity(TestEntityNoColumn.class);
	}

	@Table
	public class TestEntity implements Serializable
	{
		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		@Column
		private String name;

		@Column(name = "age_in_year")
		private Long age;

		@Column
		private List<String> friends;

		@Column
		private Set<String> followers;

		@Column
		private Map<Integer, String> preferences;

	}

}
