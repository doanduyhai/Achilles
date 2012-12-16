package fr.doan.achilles.entity;

import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.builder.PropertyMetaBuilder.builder;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.exception.InvalidBeanException;

@SuppressWarnings("unused")
public class EntityPropertyHelperTest
{

	private final EntityPropertyHelper helper = new EntityPropertyHelper();

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

	@Test(expected = InvalidBeanException.class)
	public void should_exception_when_no_getter() throws Exception
	{

		class Test
		{
			String name;
		}

		helper.findAccessors(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test(expected = InvalidBeanException.class)
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
		helper.findAccessors(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test(expected = InvalidBeanException.class)
	public void should_exception_when_non_public_getter() throws Exception
	{

		class Test
		{
			String name;

			private String getName()
			{
				return name;
			}

		}
		helper.findAccessors(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test(expected = InvalidBeanException.class)
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
		helper.findAccessors(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test(expected = InvalidBeanException.class)
	public void should_exception_when_non_public_setter() throws Exception
	{

		class Test
		{
			String name;

			public String getName()
			{
				return name;
			}

			private void setName(String name)
			{
				this.name = name;
			}
		}
		helper.findAccessors(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test(expected = InvalidBeanException.class)
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
		helper.findAccessors(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test(expected = InvalidBeanException.class)
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
		helper.findAccessors(Test.class, Test.class.getDeclaredField("name"));
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
	public void should_get_value_from_field() throws Exception
	{
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");

		String value = (String) helper.getValueFromField(bean, "complicatedAttributeName");
		assertThat(value).isEqualTo("test");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_value_from_collection_field() throws Exception
	{
		ComplexBean bean = new ComplexBean();
		bean.setFriends(Arrays.asList("foo", "bar"));
		List<String> value = (List<String>) helper.getValueFromField(bean, "friends");
		assertThat(value).containsExactly("foo", "bar");
	}

	@Test
	public void should_set_value_to_field() throws Exception
	{
		Bean bean = new Bean();

		helper.setValueToField(bean, "complicatedAttributeName", "test");
		assertThat(bean.getComplicatedAttributeName()).isEqualTo("test");
	}

	@Test
	public void should_set_value_to_collection_field() throws Exception
	{
		ComplexBean bean = new ComplexBean();
		bean.setFriends(Arrays.asList("foo", "bar"));
		helper.setValueToField(bean, "friends", new ArrayList<String>());
		assertThat(bean.getFriends()).isEmpty();
	}

	@Test
	public void should_set_null_value_to_field() throws Exception
	{
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");

		helper.setValueToField(bean, "complicatedAttributeName", null);
		assertThat(bean.getComplicatedAttributeName()).isNull();
	}

	@Test
	public void should_get_key() throws Exception
	{
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");

		Method[] accessors = helper.findAccessors(Bean.class,
				Bean.class.getDeclaredField("complicatedAttributeName"));
		PropertyMeta<Void, String> idMeta = builder(Void.class, String.class).type(SIMPLE)
				.propertyName("complicatedAttributeName").accessors(accessors).build();

		String key = helper.getKey(bean, idMeta);
		assertThat(key).isEqualTo("test");
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
