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
package info.archinnov.achilles.validation;

import static java.lang.String.format;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.AchillesInvalidTableException;

public class Validator {
	public static void validateNotBlank(String arg, String message, Object... args) {
		if (StringUtils.isBlank(arg)) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateNull(Object arg, String message, Object... args) {
		if (arg != null) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateNotNull(Object arg, String message, Object... args) {
		if (arg == null) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateNotEmpty(Collection<?> arg, String message, Object... args) {
		if (arg == null || arg.isEmpty()) {
			throw new AchillesException(format(message, args));
		}
	}

	public static <T> void validateContains(Collection<T> collection, T element, String message, Object... args) {
		if (!collection.contains(element)) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateBeanMappingNotEmpty(Collection<?> arg, String message, Object... args) {
		if (arg == null || arg.isEmpty()) {
			throw new AchillesBeanMappingException(format(message, args));
		}
	}

	public static void validateNotEmpty(Map<?, ?> arg, String message, Object... args) {
		if (arg == null || arg.isEmpty()) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateSize(Map<?, ?> arg, int size, String message, Object... args) {
		validateNotEmpty(arg, "The map '%s' should not be empty", args);
		if (arg.size() != size) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateComparable(Class<?> type, String message, Object... args) {
		if (!Comparable.class.isAssignableFrom(type)) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateNoargsConstructor(Class<?> clazz) {
		if (!clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers())) {
			Constructor<?>[] constructors = clazz.getConstructors();
			for (Constructor<?> constructor : constructors) {
				if (Modifier.isPublic(constructor.getModifiers()) && constructor.getParameterTypes().length == 0) {
					return;
				}
			}
			throw new AchillesBeanMappingException(format("The class '%s' should have a public default constructor",
					clazz.getCanonicalName()));
		}
	}

	public static void validateRegExp(String arg, String regexp, String label) {
		validateNotBlank(arg, "The text value '%s' should not be blank", label);
		if (!Pattern.matches(regexp, arg)) {
			throw new AchillesException(format("The property '%s' should match the pattern '%s'", label, regexp));
		}
	}

	public static void validateInstantiable(Class<?> arg) {
		validateNotNull(arg, "The class should not be null");
		String canonicalName = arg.getCanonicalName();
		validateNoargsConstructor(arg);
		try {
			arg.newInstance();
		} catch (InstantiationException e) {
			throw new AchillesBeanMappingException(
					format("Cannot instantiate the class '%s'. Please ensure the class is not an abstract class, an interface, an array class, a primitive type, or void and have a nullary (default) constructor and is declared public",
							canonicalName));
		} catch (IllegalAccessException e) {
			throw new AchillesBeanMappingException(
					format("Cannot instantiate the class '%s'. Please ensure the class has a public nullary (default) constructor",
							canonicalName));
		} catch (SecurityException e) {
			throw new AchillesBeanMappingException(format("Cannot instantiate the class '%s'", canonicalName));
		}
	}

	public static <T> void validateInstanceOf(Object entity, Class<T> targetClass, String message, Object... args) {
		validateNotNull(entity, "Entity '%s' should not be null", entity);
		if (!targetClass.isInstance(entity)) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateTrue(boolean condition, String message, Object... args) {
		if (!condition) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateBeanMappingTrue(boolean condition, String message, Object... args) {
		if (!condition) {
			throw new AchillesBeanMappingException(format(message, args));
		}
	}

	public static void validateTableTrue(boolean condition, String message, Object... args) {
		if (!condition) {
			throw new AchillesInvalidTableException(format(message, args));
		}
	}

	public static void validateFalse(boolean condition, String message, Object... args) {
		if (condition) {
			throw new AchillesException(format(message, args));
		}
	}

	public static void validateBeanMappingFalse(boolean condition, String message, Object... args) {
		if (condition) {
			throw new AchillesBeanMappingException(format(message, args));
		}
	}

	public static void validateTableFalse(boolean condition, String message, Object... args) {
		if (condition) {
			throw new AchillesInvalidTableException(format(message, args));
		}
	}
}
