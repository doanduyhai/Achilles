package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.entity.metadata.PropertyMetaBuilder.factory;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.helper.EntityIntrospector;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKeyByConstructor;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * AchillesMethodInvokerTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class ReflectionInvokerTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private ReflectionInvoker invoker = new ReflectionInvoker();

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
	public void should_get_partition_key() throws Exception
	{
		long partitionKey = RandomUtils.nextLong();
		Method userIdGetter = CompoundKey.class.getDeclaredMethod("getUserId");
		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compGetters(userIdGetter)
				.type(PropertyType.EMBEDDED_ID)
				.build();

		CompoundKey compoundKey = new CompoundKey(partitionKey, "name");

		assertThat(invoker.getPartitionKey(compoundKey, idMeta)).isEqualTo(partitionKey);
	}

	@Test
	public void should_return_null_for_partition_key_if_not_embedded_id() throws Exception
	{
		long partitionKey = RandomUtils.nextLong();
		Method userIdGetter = CompoundKey.class.getDeclaredMethod("getUserId");
		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compGetters(userIdGetter)
				.type(PropertyType.ID)
				.build();

		CompoundKey compoundKey = new CompoundKey(partitionKey, "name");
		assertThat(invoker.getPartitionKey(compoundKey, idMeta)).isNull();
	}

	@Test
	public void should_instanciate_entity_from_class() throws Exception
	{
		CompoundKey actual = invoker.instanciate(CompoundKey.class);
		assertThat(actual).isNotNull();
		assertThat(actual.getUserId()).isNull();
		assertThat(actual.getName()).isNull();
	}

	@Test
	public void should_instanciate_using_default_constructor() throws Exception
	{
		Constructor<CompoundKey> constructor = CompoundKey.class
				.getDeclaredConstructor();

		CompoundKey actual = invoker.instanciate(constructor);
		assertThat(actual).isNotNull();
		assertThat(actual.getUserId()).isNull();
		assertThat(actual.getName()).isNull();
	}

	@Test
	public void should_instanciate_using_custom_constructor() throws Exception
	{
		Long userId = RandomUtils.nextLong();
		String name = "name";
		Constructor<CompoundKey> constructor = CompoundKey.class
				.getDeclaredConstructor(Long.class, String.class);

		CompoundKey actual = invoker.instanciate(constructor, new Object[]
		{
				userId,
				name
		});
		assertThat(actual).isNotNull();
		assertThat(actual.getUserId()).isEqualTo(userId);
		assertThat(actual.getName()).isEqualTo(name);
	}

	@Test
	public void should_instanciate_embedded_id_with_partition_key_using_custom_constructor()
			throws Exception
	{
		Long partitionKey = RandomUtils.nextLong();

		PropertyMeta<Void, CompoundKeyByConstructor> idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKeyByConstructor.class)
				.type(PropertyType.EMBEDDED_ID)
				.compNames("toto")
				.build();

		Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class
				.getDeclaredConstructor(Long.class, String.class);
		idMeta.getCompoundKeyProperties().setConstructor(constructor);

		Object actual = invoker.instanciateEmbeddedIdWithPartitionKey(
				idMeta, partitionKey);
		idMeta.getCompoundKeyProperties().setConstructor(constructor);

		assertThat(actual).isNotNull();
		CompoundKeyByConstructor compoundKey = (CompoundKeyByConstructor) actual;
		assertThat(compoundKey.getId()).isEqualTo(partitionKey);
		assertThat(compoundKey.getName()).isNull();
	}

	@Test
	public void should_instanciate_embedded_id_with_partition_key_using_default_constructor()
			throws Exception
	{
		Long partitionKey = RandomUtils.nextLong();

		Method userIdSetter = CompoundKey.class.getDeclaredMethod("setUserId", Long.class);
		PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compSetters(userIdSetter)
				.build();
		Constructor<CompoundKey> constructor = CompoundKey.class
				.getDeclaredConstructor();
		idMeta.getCompoundKeyProperties().setConstructor(constructor);

		Object actual = invoker.instanciateEmbeddedIdWithPartitionKey(idMeta, partitionKey);

		assertThat(actual).isNotNull();
		CompoundKey compoundKey = (CompoundKey) actual;
		assertThat(compoundKey.getUserId()).isEqualTo(partitionKey);
		assertThat(compoundKey.getName()).isNull();
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
