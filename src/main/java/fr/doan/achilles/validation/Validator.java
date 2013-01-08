package fr.doan.achilles.validation;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.entity.type.MultiKey;
import fr.doan.achilles.exception.ValidationException;

public class Validator
{
	public static void validateNotBlank(String arg, String label)
	{
		if (StringUtils.isBlank(arg))
		{
			throw new ValidationException("The property '" + label + "' should not be blank");
		}
	}

	public static void validateNotNull(Object arg, String msg)
	{
		if (arg == null)
		{
			throw new ValidationException(msg);
		}
	}

	public static void validateNotEmpty(Collection<?> arg, String message)
	{
		if (arg == null || arg.isEmpty())
		{
			throw new ValidationException(message);
		}
	}

	public static void validateSize(Collection<?> arg, int size, String label)
	{
		validateNotEmpty(arg, label);
		if (arg.size() != size)
		{
			throw new ValidationException("The collection property '" + label
					+ "' should have exactly '" + size + "' elements");
		}
	}

	public static void validateNotEmpty(Map<?, ?> arg, String label)
	{
		if (arg == null || arg.isEmpty())
		{
			throw new ValidationException("The property '" + label
					+ "' should not be null or empty");
		}
	}

	public static void validateSize(Map<?, ?> arg, int size, String label)
	{
		validateNotEmpty(arg, label);
		if (arg.size() != size)
		{
			throw new ValidationException("The map property '" + label + "' should have exactly '"
					+ size + "' elements");
		}
	}

	public static void validateSerializable(Class<?> clazz, String label)
	{
		if (!Serializable.class.isAssignableFrom(clazz))
		{
			throw new ValidationException("The " + label + " should be Serializable");
		}
	}

	public static void validateAllowedTypes(Class<?> type, Set<Class<?>> allowedTypes,
			String message)
	{
		if (!allowedTypes.contains(type) && !MultiKey.class.isAssignableFrom(type))
		{
			throw new ValidationException(message);
		}
	}

	public static void validateNoargsConstructor(Class<?> clazz)
	{
		if (!clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers()))
		{
			Constructor<?>[] constructors = clazz.getConstructors();
			for (Constructor<?> constructor : constructors)
			{
				if (Modifier.isPublic(constructor.getModifiers())
						&& constructor.getParameterTypes().length == 0)
				{
					return;
				}
			}
			throw new ValidationException("The class '" + clazz.getCanonicalName()
					+ "' should have a public default constructor");
		}
	}

	public static void validateRegExp(String arg, String regexp, String label)
	{
		validateNotBlank(arg, label);
		if (!Pattern.matches(regexp, arg))
		{
			throw new ValidationException("The property '" + label + "' should match the pattern '"
					+ regexp + "'");
		}
	}

	public static void validateInstantiable(Class<?> arg)
	{
		String canonicalName = arg.getCanonicalName();
		validateNotNull(arg, canonicalName);
		validateNoargsConstructor(arg);
		try
		{
			arg.newInstance();
		}
		catch (InstantiationException e)
		{
			throw new ValidationException(
					"Cannot instantiate the class '"
							+ canonicalName
							+ "'. Please ensure the class is not an abstract class, an interface, an array class, a primitive type, or void and have a nullary (default) constructor and is declared public");
		}
		catch (IllegalAccessException e)
		{
			throw new ValidationException("Cannot instantiate the class '" + canonicalName
					+ "'. Please ensure the class has a public nullary (default) constructor");
		}
		catch (SecurityException e)
		{}
	}

	public static void validateTrue(boolean condition, String message)
	{
		if (!condition)
		{
			throw new ValidationException(message);
		}
	}

	public static void validateFalse(boolean condition, String message)
	{
		if (condition)
		{
			throw new ValidationException(message);
		}
	}
}
