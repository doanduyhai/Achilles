package fr.doan.achilles.validation;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class Validator
{
	public static void validateNotBlank(String arg, String label)
	{
		if (StringUtils.isBlank(arg))
		{
			throw new IllegalArgumentException("The constructor property '" + label + "' should not be blank");
		}
	}

	public static void validateNotNull(Object arg, String label)
	{
		if (arg == null)
		{
			throw new IllegalArgumentException("The constructor property '" + label + "' should not be null");
		}
	}

	public static void validateNotEmpty(Collection<?> arg, String label)
	{
		if (arg == null || arg.isEmpty())
		{
			throw new IllegalArgumentException("The constructor property '" + label + "' should not be null or empty");
		}
	}

	public static void validateNotEmpty(Map<?, ?> arg, String label)
	{
		if (arg == null || arg.isEmpty())
		{
			throw new IllegalArgumentException("The constructor property '" + label + "' should not be null or empty");
		}
	}

	public static void validateNoargsConstructor(Class<?> clazz)
	{
		if (!clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers()))
		{
			Constructor<?>[] constructors = clazz.getConstructors();
			for (Constructor<?> constructor : constructors)
			{
				if (Modifier.isPublic(constructor.getModifiers()) && constructor.getParameterTypes().length == 0)
				{
					return;
				}
			}
			throw new IllegalArgumentException("The class '" + clazz.getCanonicalName() + "' should have a public default constructor");
		}
	}
}
