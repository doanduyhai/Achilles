package info.archinnov.achilles.helper;

import java.lang.reflect.Field;

import com.google.common.base.Function;

/**
 * AchillesLoggerHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesLoggerHelper
{
	public static Function<Class<?>, String> fqcnToStringFn = new Function<Class<?>, String>()
	{
		public String apply(Class<?> clazz)
		{
			return clazz.getCanonicalName();
		}
	};
	public static Function<Field, String> fieldToStringFn = new Function<Field, String>()
	{
		public String apply(Field field)
		{
			return field.getName();
		}
	};
}
