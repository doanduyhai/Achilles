package fr.doan.achilles.entity;

import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.factory.PropertyMetaFactory.factory;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Id;

import mapping.entity.CompleteBean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import parser.entity.BeanWithColumnFamilyName;
import parser.entity.BeanWithNoTableAnnotation;
import parser.entity.ChildBean;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.exception.InvalidBeanException;

@SuppressWarnings("unused")
public class EntityHelperTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private final EntityHelper helper = new EntityHelper();

	@Test
	public void should_derive_getter() throws Exception
	{

		class Test
		{

			Boolean old;
		}

		assertThat(helper.deriveGetterName(Test.class.getDeclaredField("old"))).isEqualTo("getOld");
	}

	@Test
	public void should_derive_getter_for_boolean_primitive() throws Exception
	{

		class Test
		{

			boolean old;
		}

		assertThat(helper.deriveGetterName(Test.class.getDeclaredField("old"))).isEqualTo("isOld");
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

		expectedEx.expect(InvalidBeanException.class);
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

		expectedEx.expect(InvalidBeanException.class);
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
		expectedEx.expect(InvalidBeanException.class);
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
		expectedEx.expect(InvalidBeanException.class);
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

		expectedEx.expect(InvalidBeanException.class);
		expectedEx.expectMessage("The setter for field 'name' does not exist or is incorrect");

		helper.findSetter(Test.class, Test.class.getDeclaredField("name"));
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

		expectedEx.expect(IncorrectTypeException.class);
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
	public void should_exception_when_infering_column_family_for_bean_with_no_annotation()
			throws Exception
	{

		expectedEx.expect(IncorrectTypeException.class);
		expectedEx.expectMessage("The entity '"
				+ BeanWithNoTableAnnotation.class.getCanonicalName()
				+ "' should have @Table annotation");

		helper.inferColumnFamilyName(BeanWithNoTableAnnotation.class, "canonicalName");

	}

	@Test
	public void should_condition() throws Exception
	{

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
