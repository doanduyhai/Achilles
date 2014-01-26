/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.internal.reflection;

import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Field;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FieldAccessorTest {

	private FieldAccessor accessor = new FieldAccessor();

	@Test
	public void should_set_field_when_no_setter_available() throws Exception {
		// Given
		BeanWithNoSetterForField instance = new BeanWithNoSetterForField("name");
		Field nameField = BeanWithNoSetterForField.class.getDeclaredField("name");

		// When
		accessor.setValueToField(nameField, instance, "new_name");

		// Then
		assertThat(instance.getName()).isEqualTo("new_name");
	}

	@Test
	public void should_get_field_when_no_getter_available() throws Exception {
		// Given
		BeanWithNoGetterForField instance = new BeanWithNoGetterForField("name");
		Field nameField = BeanWithNoGetterForField.class.getDeclaredField("name");

		// Then
		assertThat(accessor.getValueFromField(nameField, instance)).isEqualTo("name");
	}

	public class BeanWithNoSetterForField {
		private final String name;

		public BeanWithNoSetterForField(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	public class BeanWithNoGetterForField {
		@SuppressWarnings("unused")
		private final String name;

		public BeanWithNoGetterForField(String name) {
			this.name = name;
		}

	}
}