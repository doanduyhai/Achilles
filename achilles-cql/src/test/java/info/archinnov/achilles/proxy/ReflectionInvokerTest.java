/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.entity.metadata.PropertyType.ID;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class ReflectionInvokerTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ReflectionInvoker invoker = new ReflectionInvoker();

	@Test
	public void should_get_value_from_field() throws Exception {
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");
		Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");

		String value = (String) invoker.getValueFromField(bean, getter);
		assertThat(value).isEqualTo("test");
	}

	@Test
	public void should_get_value_from_null_field() throws Exception {
		Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");
		assertThat(invoker.getValueFromField(null, getter)).isNull();
	}

	@Test
	public void should_exception_when_getting_value_from_field() throws Exception {
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");
		Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot invoke '" + getter.getName() + "' of type '" + Bean.class.getCanonicalName()
				+ "' on instance 'bean'");

		invoker.getValueFromField("bean", getter);
	}

	@Test
	public void should_set_value_to_field() throws Exception {
		Bean bean = new Bean();
		Method setter = Bean.class.getDeclaredMethod("setComplicatedAttributeName", String.class);

		invoker.setValueToField(bean, setter, "fecezzef");

		assertThat(bean.getComplicatedAttributeName()).isEqualTo("fecezzef");
	}

	@Test
	public void should_not_set_value_when_null_field() throws Exception {
		Method setter = Bean.class.getDeclaredMethod("setComplicatedAttributeName", String.class);
		invoker.setValueToField(null, setter, "fecezzef");
	}

	@Test
	public void should_exception_when_setting_value_to_field() throws Exception {
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");
		Method setter = Bean.class.getDeclaredMethod("setComplicatedAttributeName", String.class);

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot invoke '" + setter.getName() + "' of type '" + Bean.class.getCanonicalName()
				+ "' on instance 'bean'");

		invoker.setValueToField("bean", setter, "test");
	}

	@Test
	public void should_get_value_from_list_field() throws Exception {
		CompleteBean bean = new CompleteBean();
		bean.setFriends(Arrays.asList("foo", "bar"));
		Method getter = CompleteBean.class.getDeclaredMethod("getFriends");

		List<String> value = invoker.getListValueFromField(bean, getter);
		assertThat(value).containsExactly("foo", "bar");
	}

	@Test
	public void should_get_value_from_set_field() throws Exception {
		CompleteBean bean = new CompleteBean();
		bean.setFollowers(Sets.newHashSet("foo", "bar"));
		Method getter = CompleteBean.class.getDeclaredMethod("getFollowers");

		Set<String> value = invoker.getSetValueFromField(bean, getter);
		assertThat(value).containsOnly("foo", "bar");
	}

	@Test
	public void should_get_value_from_map_field() throws Exception {
		CompleteBean bean = new CompleteBean();
		bean.setPreferences(ImmutableMap.of(1, "FR"));
		Method getter = CompleteBean.class.getDeclaredMethod("getPreferences");

		Map<Integer, String> value = invoker.getMapValueFromField(bean, getter);
		assertThat(value).containsKey(1).containsValue("FR");
	}

	@Test
	public void should_get_primary_key() throws Exception {
		Long id = RandomUtils.nextLong();
		CompleteBean bean = new CompleteBean(id);

		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).type(ID).field("id")
				.accessors().build();

		Object key = invoker.getPrimaryKey(bean, idMeta);
		assertThat(key).isEqualTo(id);
	}

	@Test
	public void should_exception_when_getting_primary_key() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).type(ID).field("id")
				.accessors().build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot get primary key value by invoking getter 'getId' of type '"
				+ CompleteBean.class.getCanonicalName() + "' from entity 'bean'");

		invoker.getPrimaryKey("bean", idMeta);
	}

	@Test
	public void should_return_null_key_when_null_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").accessors()
				.build();
		assertThat(invoker.getPrimaryKey(null, idMeta)).isNull();
	}

	@Test
	public void should_get_partition_key() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		Method userIdGetter = EmbeddedKey.class.getDeclaredMethod("getUserId");
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compGetters(userIdGetter)
				.type(PropertyType.EMBEDDED_ID).build();

		EmbeddedKey embeddedKey = new EmbeddedKey(partitionKey, "name");

		assertThat(invoker.getPartitionKey(embeddedKey, idMeta)).isEqualTo(partitionKey);
	}

	@Test
	public void should_exception_when_getting_partition_key() throws Exception {

		Method userIdGetter = EmbeddedKey.class.getDeclaredMethod("getUserId");
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compGetters(userIdGetter)
				.type(PropertyType.EMBEDDED_ID).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot get partition key value by invoking getter 'getUserId' of type '"
				+ EmbeddedKey.class.getCanonicalName() + "' from compoundKey 'compound'");

		invoker.getPartitionKey("compound", idMeta);
	}

	@Test
	public void should_return_null_for_partition_key_if_not_embedded_id() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		Method userIdGetter = EmbeddedKey.class.getDeclaredMethod("getUserId");
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compGetters(userIdGetter)
				.type(PropertyType.ID).build();

		EmbeddedKey embeddedKey = new EmbeddedKey(partitionKey, "name");
		assertThat(invoker.getPartitionKey(embeddedKey, idMeta)).isNull();
	}

	@Test
	public void should_instanciate_entity_from_class() throws Exception {
		EmbeddedKey actual = invoker.instantiate(EmbeddedKey.class);
		assertThat(actual).isNotNull();
		assertThat(actual.getUserId()).isNull();
		assertThat(actual.getName()).isNull();
	}

	@Test
	public void should_instanciate_embedded_id_with_partition_key_using_default_constructor() throws Exception {
		Long partitionKey = RandomUtils.nextLong();

		Method userIdSetter = EmbeddedKey.class.getDeclaredMethod("setUserId", Long.class);
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compSetters(userIdSetter).build();

		Object actual = invoker.instantiateEmbeddedIdWithPartitionComponents(idMeta,
				Arrays.<Object> asList(partitionKey));

		assertThat(actual).isNotNull();
		EmbeddedKey embeddedKey = (EmbeddedKey) actual;
		assertThat(embeddedKey.getUserId()).isEqualTo(partitionKey);
		assertThat(embeddedKey.getName()).isNull();
	}

	@Test
	public void should_throw_exception_when_cannot_instanciate_entity_from_class() throws Exception {

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot instantiate entity from class '" + Pair.class.getCanonicalName() + "'");

		invoker.instantiate(Pair.class);
	}

	@Test
	public void should_exception_when_setting_null_to_primitive_type() throws Exception {
		Method setter = BeanWithPrimitive.class.getDeclaredMethod("setCount", int.class);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("Cannot set null value to primitive type 'int' when invoking 'setCount' on instance of class'"
						+ BeanWithPrimitive.class.getCanonicalName() + "'");

		invoker.setValueToField(new BeanWithPrimitive(), setter, null);

	}

    @Test
    public void should_instantiate_class_without_public_constructor() throws Exception {
        //When
        BeanWithoutPublicConstructor instance = invoker.instantiateImmutable(BeanWithoutPublicConstructor.class);

        //Then
        assertThat(instance).isInstanceOf(BeanWithoutPublicConstructor.class);
    }

    @Test
    public void should_set_field_when_no_setter_available() throws Exception {
        //Given
        BeanWithNoSetterForField instance = new BeanWithNoSetterForField("name");
        Field nameField = BeanWithNoSetterForField.class.getDeclaredField("name");

        //When
        invoker.setValueToFinalField(nameField,instance,"new_name");

        //Then
        assertThat(instance.getName()).isEqualTo("new_name");
    }



	private class Bean {

		private String complicatedAttributeName;

		public String getComplicatedAttributeName() {
			return complicatedAttributeName;
		}

		public void setComplicatedAttributeName(String complicatedAttributeName) {
			this.complicatedAttributeName = complicatedAttributeName;
		}
	}

	@SuppressWarnings("unused")
	private class BeanWithPrimitive {
		private int count;

		public int getCount() {
			return count;
		}

		public void setCount(int count) {
			this.count = count;
		}
	}

    public class BeanWithoutPublicConstructor {
        public BeanWithoutPublicConstructor(String name) {

        }
    }

    public class BeanWithNoSetterForField
    {
        private final String name;
        public BeanWithNoSetterForField(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
