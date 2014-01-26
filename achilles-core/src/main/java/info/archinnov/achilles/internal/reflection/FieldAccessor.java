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

import java.lang.reflect.Field;

public class FieldAccessor {

	@SuppressWarnings("unchecked")
	public <T> T getValueFromField(Field field, Object instance) throws IllegalAccessException {
		T result = null;
		if (instance != null) {
			boolean isAccessible = field.isAccessible();
			if (!isAccessible) {
				field.setAccessible(true);
			}
			result = (T) field.get(instance);
			if (!isAccessible) {
				field.setAccessible(false);
			}
		}

		return result;

	}

	public void setValueToField(Field field, Object instance, Object value) throws IllegalAccessException {
		if (instance != null) {
			boolean isAccessible = field.isAccessible();
			if (!isAccessible) {
				field.setAccessible(true);
			}
			field.set(instance, value);
			if (!isAccessible) {
				field.setAccessible(false);
			}
		}
	}
}
