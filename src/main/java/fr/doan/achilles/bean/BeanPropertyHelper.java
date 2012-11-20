package fr.doan.achilles.bean;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.exception.InvalidBeanException;
import fr.doan.achilles.metadata.PropertyMeta;

public class BeanPropertyHelper
{

	protected String deriveGetterName(Field field)
	{
		String camelCase = field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);

		if (StringUtils.equals(field.getType().toString(), "boolean"))
		{
			return "is" + camelCase;
		}
		else
		{
			return "get" + camelCase;
		}
	}

	protected String deriveSetterName(String fieldName)
	{
		return "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
	}

	public Method[] findAccessors(Class<?> beanClass, Field field)
	{

		Method[] accessors = new Method[2];

		String fieldName = field.getName();

		try
		{

			String getter = this.deriveGetterName(field);
			Method getterMethod = beanClass.getDeclaredMethod(getter, (Class<?>[]) null);
			if (!Modifier.isPublic(getterMethod.getModifiers()))
			{
				throw new InvalidBeanException("The getter for field '" + fieldName + "' should be public");
			}
			else if (getterMethod.getReturnType() != field.getType())
			{
				throw new InvalidBeanException("The getter for field '" + fieldName + "' does not return correct type");
			}
			else
			{
				accessors[0] = getterMethod;
			}

		}
		catch (NoSuchMethodException e)
		{
			throw new InvalidBeanException("The getter for field '" + fieldName + "' does not exist");
		}

		try
		{
			String setter = this.deriveSetterName(fieldName);
			Method setterMethod = beanClass.getDeclaredMethod(setter, field.getType());

			if (!Modifier.isPublic(setterMethod.getModifiers()))
			{
				throw new InvalidBeanException("The setter for field '" + fieldName + "' should be public");
			}
			else if (!setterMethod.getReturnType().toString().equals("void"))
			{
				throw new InvalidBeanException("The setter for field '" + fieldName + "' does not exist");
			}
			else
			{
				accessors[1] = setterMethod;
			}

		}
		catch (NoSuchMethodException e)
		{
			throw new InvalidBeanException("The setter for field '" + fieldName + "' does not exist or is incorrect");
		}

		return accessors;
	}

	private Field extractField(Class<?> beanClass, String fieldName)
	{
		Field field = null;
		try
		{
			field = beanClass.getDeclaredField(fieldName);
		}
		catch (SecurityException e)
		{
			throw new InvalidBeanException("The field '" + fieldName + "' cannot be accessed");
		}
		catch (NoSuchFieldException e)
		{
			throw new InvalidBeanException("The field '" + fieldName + "' does not exist");
		}
		return field;
	}

	public Object getValueFromField(Object target, String fieldName)
	{

		Object value = null;

		if (target != null)
		{
			Field field = extractField(target.getClass(), fieldName);
			String getter = deriveGetterName(field);

			try
			{
				Method getterMethod = target.getClass().getDeclaredMethod(getter, (Class<?>[]) null);
				value = getValueFromField(target, getterMethod);
			}
			catch (Exception e)
			{

			}
		}
		return value;
	}

	public Object getValueFromField(Object target, Method getter)
	{

		Object value = null;

		if (target != null)
		{

			try
			{
				value = getter.invoke(target, (Object[]) null);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
		return value;
	}

	public void setValueToField(Object target, String fieldName, Object value)
	{

		if (target != null)
		{
			Field field = extractField(target.getClass(), fieldName);
			String setter = deriveSetterName(fieldName);

			try
			{
				Method setterMethod = target.getClass().getDeclaredMethod(setter, field.getType());
				setValueToField(target, setterMethod, value);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	public void setValueToField(Object target, Method setter, Object value)
	{

		if (target != null)
		{
			try
			{
				setter.invoke(target, value);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public <ID extends Serializable> ID getKey(Object entity, PropertyMeta<ID> idMeta)
	{

		try
		{
			return (ID) idMeta.getGetter().invoke(entity, (Object[]) null);
		}
		catch (IllegalArgumentException e)
		{
			return null;
		}
		catch (IllegalAccessException e)
		{
			return null;
		}
		catch (InvocationTargetException e)
		{
			return null;
		}
	}
}
