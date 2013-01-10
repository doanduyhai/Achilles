package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.PropertyType.EXTERNAL_JOIN_WIDE_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.EXTERNAL_WIDE_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.serializer.SerializerUtils.LONG_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.STRING_SRZ;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.MERGE;
import static javax.persistence.CascadeType.PERSIST;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.Bean;
import parser.entity.BeanWithColumnFamilyName;
import parser.entity.BeanWithDuplicatedColumnName;
import parser.entity.BeanWithDuplicatedJoinColumnName;
import parser.entity.BeanWithExternalJoinWideMap;
import parser.entity.BeanWithExternalWideMap;
import parser.entity.BeanWithNoColumn;
import parser.entity.BeanWithNoId;
import parser.entity.BeanWithNotSerializableId;
import parser.entity.ChildBean;
import parser.entity.UserBean;
import parser.entity.WideRowBean;
import parser.entity.WideRowBeanWithTwoColumns;
import parser.entity.WideRowBeanWithWrongColumnType;
import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ExternalWideMapProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.AchillesException;
import fr.doan.achilles.exception.BeanMappingException;
import fr.doan.achilles.serializer.SerializerUtils;

/**
 * EntityParserTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityParserTest
{
	private final EntityParser parser = new EntityParser();

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Mock
	private Keyspace keyspace;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled;

	@Mock
	private ColumnFamilyHelper columnFamilyHelper;

	@Captor
	ArgumentCaptor<Class<?>> classCaptor;

	@Captor
	ArgumentCaptor<PropertyMeta<?, ?>> propertyMetaCaptor;

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_parse_entity() throws Exception
	{
		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace, Bean.class,
				joinPropertyMetaToBeFilled);

		assertThat(meta.getClassName()).isEqualTo("parser.entity.Bean");
		assertThat(meta.getColumnFamilyName()).isEqualTo("Bean");
		assertThat(meta.getSerialVersionUID()).isEqualTo(1L);
		assertThat(meta.getIdMeta().getValueClass()).isEqualTo(Long.class);
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getIdMeta().getValueSerializer().getComparatorType()).isEqualTo(
				LONG_SRZ.getComparatorType());
		assertThat((Serializer<Long>) meta.getIdSerializer()).isEqualTo(LONG_SRZ);
		assertThat(meta.getPropertyMetas()).hasSize(7);

		PropertyMeta<?, ?> name = meta.getPropertyMetas().get("name");
		PropertyMeta<?, ?> age = meta.getPropertyMetas().get("age_in_year");
		PropertyMeta<Void, String> friends = (PropertyMeta<Void, String>) meta.getPropertyMetas()
				.get("friends");
		PropertyMeta<Void, String> followers = (PropertyMeta<Void, String>) meta.getPropertyMetas()
				.get("followers");
		PropertyMeta<Integer, String> preferences = (PropertyMeta<Integer, String>) meta
				.getPropertyMetas().get("preferences");

		PropertyMeta<Void, UserBean> creator = (PropertyMeta<Void, UserBean>) meta
				.getPropertyMetas().get("creator");
		PropertyMeta<String, UserBean> linkedUsers = (PropertyMeta<String, UserBean>) meta
				.getPropertyMetas().get("linked_users");

		assertThat(name).isNotNull();
		assertThat(age).isNotNull();
		assertThat(friends).isNotNull();
		assertThat(followers).isNotNull();
		assertThat(preferences).isNotNull();
		assertThat(creator).isNotNull();
		assertThat(linkedUsers).isNotNull();

		assertThat(name.getPropertyName()).isEqualTo("name");
		assertThat((Class<String>) name.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) name.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(name.type()).isEqualTo(SIMPLE);

		assertThat(age.getPropertyName()).isEqualTo("age_in_year");
		assertThat((Class<Long>) age.getValueClass()).isEqualTo(Long.class);
		assertThat((Serializer<Long>) age.getValueSerializer()).isEqualTo(LONG_SRZ);
		assertThat(age.type()).isEqualTo(SIMPLE);

		assertThat(friends.getPropertyName()).isEqualTo("friends");
		assertThat((Class<String>) friends.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) friends.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(friends.type()).isEqualTo(PropertyType.LAZY_LIST);
		assertThat(friends.newListInstance()).isNotNull();
		assertThat(friends.newListInstance()).isEmpty();
		assertThat(friends.isLazy()).isTrue();
		assertThat((Class<ArrayList>) friends.newListInstance().getClass()).isEqualTo(
				ArrayList.class);

		assertThat(followers.getPropertyName()).isEqualTo("followers");
		assertThat(followers.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) followers.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(followers.type()).isEqualTo(PropertyType.SET);
		assertThat(followers.newSetInstance()).isNotNull();
		assertThat(followers.newSetInstance()).isEmpty();
		assertThat((Class<HashSet>) followers.newSetInstance().getClass()).isEqualTo(HashSet.class);

		assertThat(preferences.getPropertyName()).isEqualTo("preferences");
		assertThat(preferences.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) preferences.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(preferences.type()).isEqualTo(PropertyType.MAP);
		assertThat(preferences.getKeyClass()).isEqualTo(Integer.class);
		assertThat((Serializer<Integer>) preferences.getKeySerializer()).isEqualTo(
				SerializerUtils.INT_SRZ);
		assertThat(preferences.newMapInstance()).isNotNull();
		assertThat(preferences.newMapInstance()).isEmpty();
		assertThat((Class<HashMap>) preferences.newMapInstance().getClass()).isEqualTo(
				HashMap.class);

		assertThat(creator.getPropertyName()).isEqualTo("creator");
		assertThat(creator.getValueClass()).isEqualTo(UserBean.class);
		assertThat((Serializer) creator.getValueSerializer()).isEqualTo(SerializerUtils.OBJECT_SRZ);
		assertThat(creator.type()).isEqualTo(JOIN_SIMPLE);
		assertThat(creator.getJoinProperties().getCascadeTypes()).containsExactly(ALL);

		assertThat(linkedUsers.getPropertyName()).isEqualTo("linked_users");
		assertThat(linkedUsers.getValueClass()).isEqualTo(UserBean.class);
		assertThat((Serializer) linkedUsers.getValueSerializer()).isEqualTo(
				SerializerUtils.OBJECT_SRZ);
		assertThat(linkedUsers.type()).isEqualTo(JOIN_WIDE_MAP);
		assertThat(linkedUsers.getJoinProperties().getCascadeTypes()).containsExactly(PERSIST,
				MERGE);

		verify(joinPropertyMetaToBeFilled, times(2)).put(propertyMetaCaptor.capture(),
				classCaptor.capture());

		assertThat(classCaptor.getAllValues()).containsExactly(UserBean.class, UserBean.class);
		assertThat(propertyMetaCaptor.getAllValues()).containsExactly(creator, linkedUsers);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_entity_with_table_name() throws Exception
	{

		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace,
				BeanWithColumnFamilyName.class, joinPropertyMetaToBeFilled);

		assertThat(meta).isNotNull();
		assertThat(meta.getColumnFamilyName()).isEqualTo("myOwnCF");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_inherited_bean() throws Exception
	{
		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace, ChildBean.class,
				joinPropertyMetaToBeFilled);

		assertThat(meta).isNotNull();
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getPropertyMetas().get("name").getPropertyName()).isEqualTo("name");
		assertThat(meta.getPropertyMetas().get("address").getPropertyName()).isEqualTo("address");
		assertThat(meta.getPropertyMetas().get("nickname").getPropertyName()).isEqualTo("nickname");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_bean_with_external_wide_map() throws Exception
	{
		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace,
				BeanWithExternalWideMap.class, joinPropertyMetaToBeFilled);

		assertThat(meta).isNotNull();
		PropertyMeta<?, ?> usersPropertyMeta = meta.getPropertyMetas().get("users");
		assertThat(usersPropertyMeta.type()).isEqualTo(EXTERNAL_WIDE_MAP);
		ExternalWideMapProperties<?> externalWideMapProperties = usersPropertyMeta
				.getExternalWideMapProperties();

		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo(
				"external_users");
		assertThat(externalWideMapProperties.getExternalWideMapDao()).isNotNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_bean_with_external_join_wide_map() throws Exception
	{
		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace,
				BeanWithExternalJoinWideMap.class, joinPropertyMetaToBeFilled);

		assertThat(meta).isNotNull();
		PropertyMeta<?, ?> usersPropertyMeta = meta.getPropertyMetas().get("users");
		assertThat(usersPropertyMeta.type()).isEqualTo(EXTERNAL_JOIN_WIDE_MAP);
		ExternalWideMapProperties<?> externalWideMapProperties = usersPropertyMeta
				.getExternalWideMapProperties();

		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo(
				"external_users");
		assertThat(externalWideMapProperties.getExternalWideMapDao()).isNull();
		verify(joinPropertyMetaToBeFilled).put(usersPropertyMeta, UserBean.class);
	}

	@Test
	public void should_exception_when_entity_has_no_id() throws Exception
	{
		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("The entity '" + BeanWithNoId.class.getCanonicalName()
				+ "' should have at least one field with javax.persistence.Id annotation");
		parser.parseEntity(keyspace, BeanWithNoId.class, joinPropertyMetaToBeFilled);
	}

	@Test
	public void should_exception_when_id_type_not_serializable() throws Exception
	{
		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("Value of 'id' should be Serializable");
		parser.parseEntity(keyspace, BeanWithNotSerializableId.class, joinPropertyMetaToBeFilled);
	}

	@Test
	public void should_exception_when_entity_has_no_column() throws Exception
	{
		expectedEx.expect(BeanMappingException.class);
		expectedEx
				.expectMessage("The entity '"
						+ BeanWithNoColumn.class.getCanonicalName()
						+ "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");
		parser.parseEntity(keyspace, BeanWithNoColumn.class, joinPropertyMetaToBeFilled);
	}

	@Test
	public void should_exception_when_entity_has_duplicated_column_name() throws Exception
	{
		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The property 'name' is already used for the entity '"
				+ BeanWithDuplicatedColumnName.class.getCanonicalName() + "'");

		parser.parseEntity(keyspace, BeanWithDuplicatedColumnName.class, joinPropertyMetaToBeFilled);
	}

	@Test
	public void should_exception_when_entity_has_duplicated_join_column_name() throws Exception
	{
		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The property 'name' is already used for the entity '"
				+ BeanWithDuplicatedJoinColumnName.class.getCanonicalName() + "'");

		parser.parseEntity(keyspace, BeanWithDuplicatedJoinColumnName.class,
				joinPropertyMetaToBeFilled);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_parse_wide_row() throws Exception
	{
		EntityMeta<?> meta = parser.parseEntity(keyspace, WideRowBean.class,
				joinPropertyMetaToBeFilled);

		assertThat(meta.isWideRow()).isTrue();

		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getIdMeta().getValueClass()).isEqualTo((Class) Long.class);

		assertThat(meta.getPropertyMetas()).hasSize(1);
		assertThat(meta.getPropertyMetas().get("values").type()).isEqualTo(WIDE_MAP);
	}

	@Test
	public void should_exception_when_wide_row_more_than_one_mapped_column() throws Exception
	{
		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The WideRow entity '"
				+ WideRowBeanWithTwoColumns.class.getCanonicalName()
				+ "' should not have more than one property annotated with @Column");

		parser.parseEntity(keyspace, WideRowBeanWithTwoColumns.class, joinPropertyMetaToBeFilled);

	}

	@Test
	public void should_exception_when_wide_row_has_wrong_column_type() throws Exception
	{
		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The WideRow entity '"
				+ WideRowBeanWithWrongColumnType.class.getCanonicalName()
				+ "' should have a @Column of type WideMap");

		parser.parseEntity(keyspace, WideRowBeanWithWrongColumnType.class,
				joinPropertyMetaToBeFilled);

	}

}
