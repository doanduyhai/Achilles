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
package info.archinnov.achilles.internal.helper;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyHelperTest {
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private PropertyHelper helper;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private List<Method> componentGetters;

	@Test
	public void should_infer_value_class_from_list() throws Exception {
		@SuppressWarnings("unused")
		class Test {
			private List<String> friends;
		}

		Type type = Test.class.getDeclaredField("friends").getGenericType();

		Class<String> infered = helper.inferValueClassForListOrSet(type, Test.class);

		assertThat(infered).isEqualTo(String.class);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_infer_parameterized_value_class_from_list() throws Exception {
		@SuppressWarnings("unused")
		class Test {
			private List<Class<Void>> friends;
		}

		Type type = Test.class.getDeclaredField("friends").getGenericType();

		Class infered = helper.inferValueClassForListOrSet(type, Test.class);

		assertThat(infered).isEqualTo(Class.class);
	}

	@Test
	public void should_exception_when_infering_value_type_from_raw_list() throws Exception {
		@SuppressWarnings({ "rawtypes", "unused" })
		class Test {
			private List friends;
		}

		Type type = Test.class.getDeclaredField("friends").getGenericType();

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The type '" + type.getClass().getCanonicalName()
				+ "' of the entity 'null' should be parameterized");

		helper.inferValueClassForListOrSet(type, Test.class);

	}

	@Test
	public void should_find_index() throws Exception {
		class Test {
			@Index
			private String name;
		}

		Field field = Test.class.getDeclaredField("name");

		assertThat(helper.getIndexName(field) != null).isTrue();
	}

	@Test
	public void should_check_consistency_annotation() throws Exception {
		class Test {
			@Consistency
			private String consistency;
		}

		Field field = Test.class.getDeclaredField("consistency");

		assertThat(helper.hasConsistencyAnnotation(field)).isTrue();
	}

	@Test
	public void should_not_find_counter_if_not_long_type() throws Exception {

	}

	@Test
	public void should_return_true_when_type_supported() throws Exception {
		assertThat(PropertyHelper.isSupportedType(Long.class)).isTrue();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void should_infer_entity_class_from_interceptor() throws Exception {
		assertThat(helper.inferEntityClassFromInterceptor(longInterceptor)).isEqualTo((Class) Long.class);
	}

	private Interceptor<Long> longInterceptor = new Interceptor<Long>() {
		@Override
		public void onEvent(Long entity) {

		}

		@Override
		public List<Event> events() {
			return null;
		}
	};
}
