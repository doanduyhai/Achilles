package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.entity.metadata.PropertyMetaBuilder.factory;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.EntityIntrospector;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import mapping.entity.TweetMultiKey;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import testBuilders.PropertyMetaTestBuilder;

/**
 * AchillesMethodInvokerTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class MethodInvokerTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private MethodInvoker invoker = new MethodInvoker();

	private EntityIntrospector introspector = new EntityIntrospector();

	@Test
	public void should_get_value_from_field() throws Exception
	{
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");
		Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");

		String value = (String) invoker.getValueFromField(bean, getter);
		assertThat(value).isEqualTo("test");
	}

	@Test
	public void should_get_value_from_null_field() throws Exception
	{
		Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");
		assertThat(invoker.getValueFromField(null, getter)).isNull();
	}

	@Test
	public void should_set_value_to_field() throws Exception
	{
		Bean bean = new Bean();
		Method setter = Bean.class.getDeclaredMethod("setComplicatedAttributeName", String.class);

		invoker.setValueToField(bean, setter, "fecezzef");

		assertThat(bean.getComplicatedAttributeName()).isEqualTo("fecezzef");
	}

	@Test
	public void should_not_set_value_when_null_field() throws Exception
	{
		Method setter = Bean.class.getDeclaredMethod("setComplicatedAttributeName", String.class);
		invoker.setValueToField(null, setter, "fecezzef");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_value_from_collection_field() throws Exception
	{
		ComplexBean bean = new ComplexBean();
		bean.setFriends(Arrays.asList("foo", "bar"));
		Method getter = ComplexBean.class.getDeclaredMethod("getFriends");

		List<String> value = (List<String>) invoker.getValueFromField(bean, getter);
		assertThat(value).containsExactly("foo", "bar");
	}

	@Test
	public void should_get_key() throws Exception
	{
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");

		Method[] accessors = introspector.findAccessors(Bean.class,
				Bean.class.getDeclaredField("complicatedAttributeName"));
		PropertyMeta<Void, String> idMeta = factory()
				.type(SIMPLE)
				.propertyName("complicatedAttributeName")
				.accessors(accessors)
				.build(Void.class, String.class);

		Object key = invoker.getPrimaryKey(bean, idMeta);
		assertThat(key).isEqualTo("test");
	}

	@Test
	public void should_return_null_key_when_null_entity() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class)
				.field("id")
				.accessors()
				.build();
		assertThat(invoker.getPrimaryKey(null, idMeta)).isNull();
	}

	@Test
	public void should_determine_multikey() throws Exception
	{
		Method idGetter = TweetMultiKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetMultiKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetMultiKey.class.getDeclaredMethod("getRetweetCount");

		TweetMultiKey multiKey = new TweetMultiKey();
		UUID uuid = new UUID(10L, 100L);

		multiKey.setId(uuid);
		multiKey.setAuthor("author");
		multiKey.setRetweetCount(12);

		List<Object> multiKeyList = invoker.determineMultiKeyValues(multiKey,
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
		UUID uuid = new UUID(10L, 100L);

		multiKey.setId(uuid);
		multiKey.setRetweetCount(12);

		List<Object> multiKeyList = invoker.determineMultiKeyValues(multiKey,
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		assertThat(multiKeyList).hasSize(3);
		assertThat(multiKeyList.get(0)).isEqualTo(uuid);
		assertThat(multiKeyList.get(1)).isNull();
		assertThat(multiKeyList.get(2)).isEqualTo(12);
	}

	@Test
	public void should_return_empty_multikey_when_null_entity() throws Exception
	{
		assertThat(invoker.determineMultiKeyValues(null, new ArrayList<Method>())).isEmpty();
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
