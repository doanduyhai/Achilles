package info.archinnov.achilles.entity;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.factory.PropertyMetaFactory.factory;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ANY;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.exception.AchillesBeanMappingException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import mapping.entity.CompleteBean;
import mapping.entity.TweetMultiKey;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.BeanWithColumnFamilyName;
import parser.entity.ChildBean;

/**
 * EntityHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings("unused")
@RunWith(MockitoJUnitRunner.class)
public class EntityIntrospectorTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private PropertyMeta<TweetMultiKey, String> wideMapMeta;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> getterMetas;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> setterMetas;

	@Mock
	private GenericEntityDao<Long> dao;

	private final EntityIntrospector helper = new EntityIntrospector();

	@Test
	public void should_derive_getter() throws Exception
	{

		class Test
		{

			Boolean old;
		}

		String[] getterNames = helper.deriveGetterName(Test.class.getDeclaredField("old"));
		assertThat(getterNames).hasSize(1);
		assertThat(getterNames[0]).isEqualTo("getOld");
	}

	@Test
	public void should_derive_getter_for_boolean_primitive() throws Exception
	{

		class Test
		{

			boolean old;
		}

		String[] getterNames = helper.deriveGetterName(Test.class.getDeclaredField("old"));
		assertThat(getterNames).hasSize(2);
		assertThat(getterNames[0]).isEqualTo("isOld");
		assertThat(getterNames[1]).isEqualTo("getOld");
	}

	@Test
	public void should_derive_setter() throws Exception
	{
		class Test
		{

			boolean a;
		}

		assertThat(helper.deriveSetterName("a")).isEqualTo("setA");
	}

	@Test
	public void should_exception_when_no_getter() throws Exception
	{

		class Test
		{
			String name;
		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The getter for field 'name' does not exist");

		helper.findGetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_no_setter() throws Exception
	{

		class Test
		{
			String name;

			public String getA()
			{
				return name;
			}
		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The setter for field 'name' does not exist");

		helper.findSetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_incorrect_getter() throws Exception
	{

		class Test
		{
			String name;

			public Long getName()
			{
				return 1L;
			}

		}
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The getter for field 'name' does not return correct type");

		helper.findGetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_setter_returning_wrong_type() throws Exception
	{

		class Test
		{
			String name;

			public String getName()
			{
				return name;
			}

			public Long setName(String name)
			{
				return 1L;
			}

		}
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("The setter for field 'name' does not return correct type or does not have the correct parameter");

		helper.findSetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_setter_taking_wrong_type() throws Exception
	{

		class Test
		{
			String name;

			public String getName()
			{
				return name;
			}

			public void setName(Long name)
			{}

		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The setter for field 'name' does not exist or is incorrect");

		helper.findSetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_find_getter_from_boolean_as_isOld() throws Exception
	{
		class Test
		{
			boolean old;

			public boolean isOld()
			{
				return old;
			}

			public void setOld(boolean old)
			{
				this.old = old;
			}
		}

		Method[] accessors = helper.findAccessors(Test.class, Test.class.getDeclaredField("old"));

		assertThat(accessors[0].getName()).isEqualTo("isOld");
	}

	@Test
	public void should_find_getter_from_boolean_as_getOld() throws Exception
	{
		class Test
		{
			boolean old;

			public boolean getOld()
			{
				return old;
			}

			public void setOld(boolean old)
			{
				this.old = old;
			}
		}

		Method[] accessors = helper.findAccessors(Test.class, Test.class.getDeclaredField("old"));

		assertThat(accessors[0].getName()).isEqualTo("getOld");
	}

	@Test
	public void should_find_accessors() throws Exception
	{

		Method[] accessors = helper.findAccessors(Bean.class,
				Bean.class.getDeclaredField("complicatedAttributeName"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getComplicatedAttributeName");
		assertThat(accessors[1].getName()).isEqualTo("setComplicatedAttributeName");
	}

	@Test
	public void should_find_accessors_from_collection_types() throws Exception
	{

		Method[] accessors = helper.findAccessors(ComplexBean.class,
				ComplexBean.class.getDeclaredField("friends"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getFriends");
		assertThat(accessors[1].getName()).isEqualTo("setFriends");
	}

	@Test
	public void should_find_accessors_from_widemap_type() throws Exception
	{
		Method[] accessors = helper.findAccessors(CompleteBean.class,
				CompleteBean.class.getDeclaredField("tweets"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getTweets");
		assertThat(accessors[1]).isNull();
	}

	@Test
	public void should_get_value_from_field() throws Exception
	{
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");
		Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");

		String value = (String) helper.getValueFromField(bean, getter);
		assertThat(value).isEqualTo("test");
	}

	@Test
	public void should_get_value_from_null_field() throws Exception
	{
		Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");
		assertThat(helper.getValueFromField(null, getter)).isNull();
	}

	@Test
	public void should_set_value_to_field() throws Exception
	{
		Bean bean = new Bean();
		Method setter = Bean.class.getDeclaredMethod("setComplicatedAttributeName", String.class);

		helper.setValueToField(bean, setter, "fecezzef");

		assertThat(bean.getComplicatedAttributeName()).isEqualTo("fecezzef");
	}

	@Test
	public void should_not_set_value_when_null_field() throws Exception
	{
		Method setter = Bean.class.getDeclaredMethod("setComplicatedAttributeName", String.class);
		helper.setValueToField(null, setter, "fecezzef");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_value_from_collection_field() throws Exception
	{
		ComplexBean bean = new ComplexBean();
		bean.setFriends(Arrays.asList("foo", "bar"));
		Method getter = ComplexBean.class.getDeclaredMethod("getFriends");

		List<String> value = (List<String>) helper.getValueFromField(bean, getter);
		assertThat(value).containsExactly("foo", "bar");
	}

	@Test
	public void should_get_key() throws Exception
	{
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");

		Method[] accessors = helper.findAccessors(Bean.class,
				Bean.class.getDeclaredField("complicatedAttributeName"));
		PropertyMeta<Void, String> idMeta = factory(Void.class, String.class).type(SIMPLE)
				.propertyName("complicatedAttributeName").accessors(accessors).build();

		String key = helper.getKey(bean, idMeta);
		assertThat(key).isEqualTo("test");
	}

	@Test
	public void should_return_null_key_when_null_entity() throws Exception
	{
		PropertyMeta<Void, String> idMeta = new PropertyMeta<Void, String>();
		assertThat(helper.getKey(null, idMeta)).isNull();
	}

	@Test
	public void should_get_inherited_fields() throws Exception
	{
		List<Field> fields = helper.getInheritedPrivateFields(ChildBean.class);

		assertThat(fields).hasSize(4);
		assertThat(fields.get(0).getName()).isEqualTo("nickname");
		assertThat(fields.get(1).getName()).isEqualTo("name");
		assertThat(fields.get(2).getName()).isEqualTo("address");
		assertThat(fields.get(3).getName()).isEqualTo("id");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_get_inherited_field_by_annotation() throws Exception
	{
		Field id = helper.getInheritedPrivateFields(ChildBean.class, Id.class);

		assertThat(id.getName()).isEqualTo("id");
		assertThat(id.getType()).isEqualTo((Class) Long.class);
	}

	@Test
	public void should_not_get_inherited_field_by_annotation_when_no_match() throws Exception
	{
		assertThat(helper.getInheritedPrivateFields(ChildBean.class, javax.persistence.Basic.class))
				.isNull();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_get_inherited_field_by_annotation_and_name() throws Exception
	{
		Field address = helper.getInheritedPrivateFields(ChildBean.class, Column.class, "address");

		assertThat(address.getName()).isEqualTo("address");
		assertThat(address.getType()).isEqualTo((Class) String.class);
	}

	@Test
	public void should_not_get_inherited_field_by_annotation_and_name_when_no_match()
			throws Exception
	{
		assertThat(
				helper.getInheritedPrivateFields(ChildBean.class, javax.persistence.Basic.class,
						"address")).isNull();
	}

	@Test
	public void should_find_serial_version_UID() throws Exception
	{
		class Test
		{
			private static final long serialVersionUID = 1542L;
		}

		Long serialUID = helper.findSerialVersionUID(Test.class);
		assertThat(serialUID).isEqualTo(1542L);
	}

	@Test
	public void should_exception_when_no_serial_version_UID() throws Exception
	{
		class Test
		{
			private static final long fieldName = 1542L;
		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("The 'serialVersionUID' property should be declared for entity 'null'");

		helper.findSerialVersionUID(Test.class);
	}

	@Test
	public void should_infer_column_family_from_annotation() throws Exception
	{
		String cfName = helper.inferColumnFamilyName(BeanWithColumnFamilyName.class,
				"canonicalName");
		assertThat(cfName).isEqualTo("myOwnCF");
	}

	@Test
	public void should_infer_column_family_from_default_name() throws Exception
	{
		String cfName = helper.inferColumnFamilyName(CompleteBean.class, "canonicalName");
		assertThat(cfName).isEqualTo("canonicalName");
	}

	@Test
	public void should_infer_column_family_from_default_name_when_empty_annotation_name()
			throws Exception
	{
		@Table(name = "")
		class Test
		{

		}
		String cfName = helper.inferColumnFamilyName(Test.class, "canonicalName");
		assertThat(cfName).isEqualTo("canonicalName");
	}

	@Test
	public void should_determine_multikey() throws Exception
	{
		Method idGetter = TweetMultiKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetMultiKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetMultiKey.class.getDeclaredMethod("getRetweetCount");

		TweetMultiKey multiKey = new TweetMultiKey();
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		multiKey.setId(uuid);
		multiKey.setAuthor("author");
		multiKey.setRetweetCount(12);

		List<Object> multiKeyList = helper.determineMultiKey(multiKey,
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		assertThat(multiKeyList).hasSize(3);
		assertThat(multiKeyList.get(0)).isEqualTo(uuid);
		assertThat(multiKeyList.get(1)).isEqualTo("author");
		assertThat(multiKeyList.get(2)).isEqualTo(12);
	}

	@Test
	public void should_determine_multikey_with_null() throws Exception
	{
		Method idGetter = TweetMultiKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetMultiKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetMultiKey.class.getDeclaredMethod("getRetweetCount");

		TweetMultiKey multiKey = new TweetMultiKey();
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		multiKey.setId(uuid);
		multiKey.setRetweetCount(12);

		List<Object> multiKeyList = helper.determineMultiKey(multiKey,
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		assertThat(multiKeyList).hasSize(3);
		assertThat(multiKeyList.get(0)).isEqualTo(uuid);
		assertThat(multiKeyList.get(1)).isNull();
		assertThat(multiKeyList.get(2)).isEqualTo(12);
	}

	@Test
	public void should_return_empty_multikey_when_null_entity() throws Exception
	{
		assertThat(helper.determineMultiKey(null, new ArrayList<Method>())).isEmpty();
	}

	@Test
	public void should_find_any_any_consistency_level() throws Exception
	{
		@Consistency(read = ANY, write = LOCAL_QUORUM)
		class Test
		{}

		Pair<ConsistencyLevel, ConsistencyLevel> levels = helper.findConsistencyLevels(Test.class);

		assertThat(levels.left).isEqualTo(ANY);
		assertThat(levels.right).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_find_one_one_consistency_level_by_default() throws Exception
	{
		class Test
		{}

		Pair<ConsistencyLevel, ConsistencyLevel> levels = helper.findConsistencyLevels(Test.class);

		assertThat(levels.left).isEqualTo(QUORUM);
		assertThat(levels.right).isEqualTo(QUORUM);
	}

	class Bean
	{

		private String complicatedAttributeName;

		public String getComplicatedAttributeName()
		{
			return complicatedAttributeName;
		}

		public void setComplicatedAttributeName(String complicatedAttributeName)
		{
			this.complicatedAttributeName = complicatedAttributeName;
		}
	}

	class ComplexBean
	{
		private List<String> friends;

		public List<String> getFriends()
		{
			return friends;
		}

		public void setFriends(List<String> friends)
		{
			this.friends = friends;
		}
	}
}
