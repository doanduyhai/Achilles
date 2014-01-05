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
package info.archinnov.achilles.internal.reflection;

import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.ID;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

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
    public void should_get_value_from_field_by_getter() throws Exception {
        Bean bean = new Bean();
        bean.setComplicatedAttributeName("test");
        Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");

        String value = (String) invoker.getValueFromField(bean, getter);
        assertThat(value).isEqualTo("test");
    }

    @Test
    public void should_get_value_from_null_field_by_getter() throws Exception {
        Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");
        assertThat(invoker.getValueFromField(null, getter)).isNull();
    }

    @Test
    public void should_exception_when_getting_value_from_field_by_getter() throws Exception {
        Bean bean = new Bean();
        bean.setComplicatedAttributeName("test");
        Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");

        exception.expect(AchillesException.class);
        exception.expectMessage("Cannot invoke '" + getter.getName() + "' of type '" + Bean.class.getCanonicalName()
                                        + "' on instance 'bean'");

        invoker.getValueFromField("bean", getter);
    }

	@Test
	public void should_get_value_from_field() throws Exception {
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");
		Field field = Bean.class.getDeclaredField("complicatedAttributeName");

		String value =  invoker.getValueFromField(bean, field);
		assertThat(value).isEqualTo("test");
	}

	@Test
	public void should_get_value_from_null_field() throws Exception {
        Field field = Bean.class.getDeclaredField("complicatedAttributeName");
		assertThat(invoker.getValueFromField(null, field)).isNull();
	}

	@Test
	public void should_exception_when_getting_value_from_field() throws Exception {
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");
        Field field = Bean.class.getDeclaredField("complicatedAttributeName");

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot get value from field '" + field.getName() + "' of type '" + Bean.class.getCanonicalName()
				+ "' on instance 'bean'");

		invoker.getValueFromField("bean", field);
	}

	@Test
	public void should_set_value_to_field() throws Exception {
		Bean bean = new Bean();
        Field field = Bean.class.getDeclaredField("complicatedAttributeName");

		invoker.setValueToField(bean, field, "fecezzef");

		assertThat(bean.getComplicatedAttributeName()).isEqualTo("fecezzef");
	}

	@Test
	public void should_not_set_value_when_null_field() throws Exception {
        Field field = Bean.class.getDeclaredField("complicatedAttributeName");
		invoker.setValueToField(null, field, "fecezzef");
	}

	@Test
	public void should_exception_when_setting_value_to_field() throws Exception {
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");
        Field field = Bean.class.getDeclaredField("complicatedAttributeName");

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot set value to field '" + field.getName() + "' of type '" + Bean.class.getCanonicalName()
				+ "' on instance 'bean'");

		invoker.setValueToField("bean", field, "test");
	}

	@Test
	public void should_get_value_from_list_field() throws Exception {
		CompleteBean bean = new CompleteBean();
		bean.setFriends(Arrays.asList("foo", "bar"));
		Field field = CompleteBean.class.getDeclaredField("friends");

		List<String> value = invoker.getListValueFromField(bean, field);
		assertThat(value).containsExactly("foo", "bar");
	}

	@Test
	public void should_get_value_from_set_field() throws Exception {
		CompleteBean bean = new CompleteBean();
		bean.setFollowers(Sets.newHashSet("foo", "bar"));
        Field field = CompleteBean.class.getDeclaredField("followers");

		Set<String> value = invoker.getSetValueFromField(bean, field);
		assertThat(value).containsOnly("foo", "bar");
	}

	@Test
	public void should_get_value_from_map_field() throws Exception {
		CompleteBean bean = new CompleteBean();
		bean.setPreferences(ImmutableMap.of(1, "FR"));
        Field field = CompleteBean.class.getDeclaredField("preferences");

		Map<Integer, String> value = invoker.getMapValueFromField(bean, field);
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
		exception.expectMessage("Cannot get primary key from field 'id' of type '"
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
		Field userIdField = EmbeddedKey.class.getDeclaredField("userId");
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class)
            .compFields(userIdField).type(PropertyType.EMBEDDED_ID).build();

		EmbeddedKey embeddedKey = new EmbeddedKey(partitionKey, "name");

		assertThat(invoker.getPartitionKey(embeddedKey, idMeta)).isEqualTo(partitionKey);
	}

	@Test
	public void should_exception_when_getting_partition_key() throws Exception {

        Field userIdField = EmbeddedKey.class.getDeclaredField("userId");
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class)
            .compFields(userIdField).type(PropertyType.EMBEDDED_ID).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot get partition key from field 'userId' of type '"
				+ EmbeddedKey.class.getCanonicalName() + "' from compoundKey 'compound'");

		invoker.getPartitionKey("compound", idMeta);
	}

	@Test
	public void should_return_null_for_partition_key_if_not_embedded_id() throws Exception {
		long partitionKey = RandomUtils.nextLong();
        Field userIdField = EmbeddedKey.class.getDeclaredField("userId");
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class)
            .compFields(userIdField).type(PropertyType.ID).build();

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

        Field userIdField = EmbeddedKey.class.getDeclaredField("userId");
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compFields(userIdField).build();

		Object actual = invoker.instantiateEmbeddedIdWithPartitionComponents(idMeta,
				Arrays.<Object> asList(partitionKey));

		assertThat(actual).isNotNull();
		EmbeddedKey embeddedKey = (EmbeddedKey) actual;
		assertThat(embeddedKey.getUserId()).isEqualTo(partitionKey);
		assertThat(embeddedKey.getName()).isNull();
	}

	@Test
	public void should_exception_when_setting_null_to_primitive_type() throws Exception {
        Field field = BeanWithPrimitive.class.getDeclaredField("count");

		exception.expect(AchillesException.class);
		exception
				.expectMessage("Cannot set null value to primitive type 'int' of field 'count' on instance of class'"
						+ BeanWithPrimitive.class.getCanonicalName() + "'");

		invoker.setValueToField(new BeanWithPrimitive(), field, null);
	}

    @Test
    public void should_instantiate_class_without_public_constructor() throws Exception {
        //When
        BeanWithoutPublicConstructor instance = invoker.instantiate(BeanWithoutPublicConstructor.class);

        //Then
        assertThat(instance).isInstanceOf(BeanWithoutPublicConstructor.class);
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
}
