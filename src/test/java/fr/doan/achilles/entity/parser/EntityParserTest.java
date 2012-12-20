package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

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
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.Bean;
import parser.entity.BeanWithColumnFamilyName;
import parser.entity.BeanWithNoColumn;
import parser.entity.BeanWithNoId;
import parser.entity.BeanWithNotSerializableId;
import parser.entity.ChildBean;
import parser.entity.WideRowBean;
import parser.entity.WideRowBeanWithTwoColumns;
import parser.entity.WideRowBeanWithWrongColumnType;
import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ListMeta;
import fr.doan.achilles.entity.metadata.MapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.metadata.SetMeta;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.serializer.Utils;

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
	private ColumnFamilyHelper columnFamilyHelper;

	private boolean forceColumnFamilyCreation = true;

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_parse_entity() throws Exception
	{
		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace, Bean.class,
				entityMetaMap, columnFamilyHelper, forceColumnFamilyCreation);

		assertThat(meta.getCanonicalClassName()).isEqualTo("parser.entity.Bean");
		assertThat(meta.getColumnFamilyName()).isEqualTo("parser_entity_Bean");
		assertThat(meta.getSerialVersionUID()).isEqualTo(1L);
		assertThat(meta.getIdMeta().getValueClass()).isEqualTo(Long.class);
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getIdMeta().getValueSerializer().getComparatorType()).isEqualTo(
				LONG_SRZ.getComparatorType());
		assertThat((Serializer<Long>) meta.getIdSerializer()).isEqualTo(LONG_SRZ);
		assertThat(meta.getPropertyMetas()).hasSize(5);

		PropertyMeta<?, ?> name = meta.getPropertyMetas().get("name");
		PropertyMeta<?, ?> age = meta.getPropertyMetas().get("age_in_year");
		ListMeta<String> friends = (ListMeta<String>) meta.getPropertyMetas().get("friends");
		SetMeta<String> followers = (SetMeta<String>) meta.getPropertyMetas().get("followers");
		MapMeta<Integer, String> preferences = (MapMeta<Integer, String>) meta.getPropertyMetas()
				.get("preferences");

		assertThat(name).isNotNull();
		assertThat(age).isNotNull();
		assertThat(friends).isNotNull();
		assertThat(followers).isNotNull();
		assertThat(preferences).isNotNull();

		assertThat(name.getPropertyName()).isEqualTo("name");
		assertThat((Class<String>) name.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) name.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(name.propertyType()).isEqualTo(SIMPLE);

		assertThat(age.getPropertyName()).isEqualTo("age_in_year");
		assertThat((Class<Long>) age.getValueClass()).isEqualTo(Long.class);
		assertThat((Serializer<Long>) age.getValueSerializer()).isEqualTo(LONG_SRZ);
		assertThat(age.propertyType()).isEqualTo(SIMPLE);

		assertThat(friends.getPropertyName()).isEqualTo("friends");
		assertThat((Class<String>) friends.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) friends.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(friends.propertyType()).isEqualTo(PropertyType.LAZY_LIST);
		assertThat(friends.newListInstance()).isNotNull();
		assertThat(friends.newListInstance()).isEmpty();
		assertThat(friends.isLazy()).isTrue();
		assertThat((Class<ArrayList>) friends.newListInstance().getClass()).isEqualTo(
				ArrayList.class);

		assertThat(followers.getPropertyName()).isEqualTo("followers");
		assertThat(followers.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) followers.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(followers.propertyType()).isEqualTo(PropertyType.SET);
		assertThat(followers.newSetInstance()).isNotNull();
		assertThat(followers.newSetInstance()).isEmpty();
		assertThat((Class<HashSet>) followers.newSetInstance().getClass()).isEqualTo(HashSet.class);

		assertThat(preferences.getPropertyName()).isEqualTo("preferences");
		assertThat(preferences.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) preferences.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(preferences.propertyType()).isEqualTo(PropertyType.MAP);
		assertThat(preferences.getKeyClass()).isEqualTo(Integer.class);
		assertThat((Serializer<Integer>) preferences.getKeySerializer()).isEqualTo(Utils.INT_SRZ);
		assertThat(preferences.newMapInstance()).isNotNull();
		assertThat(preferences.newMapInstance()).isEmpty();
		assertThat((Class<HashMap>) preferences.newMapInstance().getClass()).isEqualTo(
				HashMap.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_entity_with_table_name() throws Exception
	{

		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace,
				BeanWithColumnFamilyName.class, entityMetaMap, columnFamilyHelper,
				forceColumnFamilyCreation);

		assertThat(meta).isNotNull();
		assertThat(meta.getColumnFamilyName()).isEqualTo("myOwnCF");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_inherited_bean() throws Exception
	{
		EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace, ChildBean.class,
				entityMetaMap, columnFamilyHelper, forceColumnFamilyCreation);

		assertThat(meta).isNotNull();
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getPropertyMetas().get("name").getPropertyName()).isEqualTo("name");
		assertThat(meta.getPropertyMetas().get("address").getPropertyName()).isEqualTo("address");
		assertThat(meta.getPropertyMetas().get("nickname").getPropertyName()).isEqualTo("nickname");
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_id() throws Exception
	{
		parser.parseEntity(keyspace, BeanWithNoId.class, entityMetaMap, columnFamilyHelper,
				forceColumnFamilyCreation);
	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_id_type_not_serializable() throws Exception
	{
		parser.parseEntity(keyspace, BeanWithNotSerializableId.class, entityMetaMap,
				columnFamilyHelper, forceColumnFamilyCreation);
	}

	@Test(expected = IncorrectTypeException.class)
	public void should_exception_when_entity_has_no_column() throws Exception
	{
		parser.parseEntity(keyspace, BeanWithNoColumn.class, entityMetaMap, columnFamilyHelper,
				forceColumnFamilyCreation);
	}

	@Test
	public void should_parse_wide_row() throws Exception
	{
		EntityMeta<?> meta = parser.parseEntity(keyspace, WideRowBean.class, entityMetaMap,
				columnFamilyHelper, forceColumnFamilyCreation);

		assertThat(meta.isWideRow()).isTrue();

		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getIdMeta().getValueClass()).isEqualTo((Class) Long.class);

		assertThat(meta.getPropertyMetas()).hasSize(1);
		assertThat(meta.getPropertyMetas().get("values").propertyType()).isEqualTo(WIDE_MAP);
	}

	@Test
	public void should_exception_when_wide_row_more_than_one_mapped_column() throws Exception
	{
		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The WideRow entity '"
				+ WideRowBeanWithTwoColumns.class.getCanonicalName()
				+ "' should not have more than one property annotated with @Column");

		parser.parseEntity(keyspace, WideRowBeanWithTwoColumns.class, entityMetaMap,
				columnFamilyHelper, forceColumnFamilyCreation);

	}

	@Test
	public void should_exception_when_wide_row_has_wrong_column_type() throws Exception
	{
		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The WideRow entity '"
				+ WideRowBeanWithWrongColumnType.class.getCanonicalName()
				+ "' should have a @Column of type WideMap");

		parser.parseEntity(keyspace, WideRowBeanWithWrongColumnType.class, entityMetaMap,
				columnFamilyHelper, forceColumnFamilyCreation);

	}

}
