package fr.doan.achilles.entity;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Table;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.parser.PropertyFilter;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.exception.InvalidBeanException;

public class EntityHelper
{

	private PropertyFilter filter = new PropertyFilter();

	protected String deriveGetterName(Field field)
	{
		String camelCase = field.getName().substring(0, 1).toUpperCase()
				+ field.getName().substring(1);

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

	public Method findGetter(Class<?> beanClass, Field field)
	{
		String fieldName = field.getName();

		try
		{

			String getter = this.deriveGetterName(field);
			Method getterMethod = beanClass.getMethod(getter);
			if (getterMethod.getReturnType() != field.getType())
			{
				throw new InvalidBeanException("The getter for field '" + fieldName
						+ "' does not return correct type");
			}

			return getterMethod;

		}
		catch (NoSuchMethodException e)
		{
			throw new InvalidBeanException("The getter for field '" + fieldName
					+ "' does not exist");
		}
	}

	public Method findSetter(Class<?> beanClass, Field field)
	{
		String fieldName = field.getName();

		try
		{
			String setter = this.deriveSetterName(fieldName);
			Method setterMethod = beanClass.getMethod(setter, field.getType());

			if (!setterMethod.getReturnType().toString().equals("void"))
			{
				throw new InvalidBeanException("The setter for field '" + fieldName
						+ "' does not return correct type or does not have the correct parameter");
			}

			return setterMethod;

		}
		catch (NoSuchMethodException e)
		{
			throw new InvalidBeanException("The setter for field '" + fieldName
					+ "' does not exist or is incorrect");
		}
	}

	public Method[] findAccessors(Class<?> beanClass, Field field)
	{

		Method[] accessors = new Method[2];

		accessors[0] = findGetter(beanClass, field);
		accessors[1] = findSetter(beanClass, field);

		return accessors;
	}

	public Object getValueFromField(Object target, Method getter)
	{

		Object value = null;

		if (target != null)
		{

			try
			{
				value = getter.invoke(target);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	public <ID> ID getKey(Object entity, PropertyMeta<Void, ID> idMeta)
	{

		try
		{
			return (ID) idMeta.getGetter().invoke(entity);
		}
		catch (Exception e)
		{
			return null;
		}
	}

	public Long findSerialVersionUID(Class<?> entity)
	{
		Long serialVersionUID = null;
		try
		{
			Field declaredSerialVersionUID = entity.getDeclaredField("serialVersionUID");
			declaredSerialVersionUID.setAccessible(true);
			serialVersionUID = declaredSerialVersionUID.getLong(null);

		}
		catch (NoSuchFieldException e)
		{
			throw new IncorrectTypeException(
					"The 'serialVersionUID' property should be declared for entity '"
							+ entity.getCanonicalName() + "'", e);
		}
		catch (IllegalAccessException e)
		{
			throw new IncorrectTypeException(
					"The 'serialVersionUID' property should be publicly accessible for entity '"
							+ entity.getCanonicalName() + "'", e);
		}
		return serialVersionUID;
	}

	public String inferColumnFamilyName(Class<?> entity, String canonicalName)
	{
		Table table = entity.getAnnotation(javax.persistence.Table.class);
		String columnFamily;
		if (table != null)
		{
			if (StringUtils.isNotBlank(table.name()))
			{
				columnFamily = table.name();

			}
			else
			{
				columnFamily = canonicalName;
			}
		}
		else
		{
			throw new IncorrectTypeException("The entity '" + entity.getCanonicalName()
					+ "' should have @Table annotation");
		}
		return columnFamily;
	}

	public List<Field> getInheritedPrivateFields(Class<?> type)
	{
		List<Field> result = new ArrayList<Field>();

		Class<?> i = type;
		while (i != null && i != Object.class)
		{
			for (Field declaredField : i.getDeclaredFields())
			{
				if (filter.matches(declaredField))
				{
					result.add(declaredField);
				}
			}
			i = i.getSuperclass();
		}

		return result;
	}

	public Field getInheritedPrivateFields(Class<?> type, Class<?> annotation)
	{
		Class<?> i = type;
		while (i != null && i != Object.class)
		{
			for (Field declaredField : i.getDeclaredFields())
			{
				if (filter.matches(declaredField, annotation))
				{
					return declaredField;
				}
			}
			i = i.getSuperclass();
		}
		return null;
	}

	public Field getInheritedPrivateFields(Class<?> type, Class<?> annotation, String name)
	{
		Class<?> i = type;
		while (i != null && i != Object.class)
		{
			for (Field declaredField : i.getDeclaredFields())
			{
				if (filter.matches(declaredField, annotation, name))
				{
					return declaredField;
				}
			}
			i = i.getSuperclass();
		}
		return null;
	}
}
