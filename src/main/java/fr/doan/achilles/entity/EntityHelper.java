package fr.doan.achilles.entity;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Table;

import net.sf.cglib.proxy.Factory;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.parser.PropertyFilter;
import fr.doan.achilles.entity.type.WideRow;
import fr.doan.achilles.exception.AchillesException;
import fr.doan.achilles.exception.BeanMappingException;
import fr.doan.achilles.proxy.interceptor.AchillesInterceptor;

/**
 * EntityHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityHelper
{

	private static final Logger log = LoggerFactory.getLogger(EntityHelper.class);

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
		log.trace("Find getter for field {} in bean {}", field.getName(),
				beanClass.getCanonicalName());

		String fieldName = field.getName();

		try
		{

			String getter = this.deriveGetterName(field);
			Method getterMethod = beanClass.getMethod(getter);
			if (getterMethod.getReturnType() != field.getType())
			{
				throw new BeanMappingException("The getter for field '" + fieldName
						+ "' does not return correct type");
			}

			return getterMethod;

		}
		catch (NoSuchMethodException e)
		{
			throw new BeanMappingException("The getter for field '" + fieldName
					+ "' does not exist");
		}
	}

	public Method findSetter(Class<?> beanClass, Field field)
	{
		log.trace("Find setter for field {} in bean {}", field.getName(),
				beanClass.getCanonicalName());

		String fieldName = field.getName();

		try
		{
			String setter = this.deriveSetterName(fieldName);
			Method setterMethod = beanClass.getMethod(setter, field.getType());

			if (!setterMethod.getReturnType().toString().equals("void"))
			{
				throw new BeanMappingException("The setter for field '" + fieldName
						+ "' does not return correct type or does not have the correct parameter");
			}

			return setterMethod;

		}
		catch (NoSuchMethodException e)
		{
			throw new BeanMappingException("The setter for field '" + fieldName
					+ "' does not exist or is incorrect");
		}
	}

	public Method[] findAccessors(Class<?> beanClass, Field field)
	{

		Method[] accessors = new Method[2];

		accessors[0] = findGetter(beanClass, field);
		if (field.getType() != WideRow.class)
		{
			accessors[1] = findSetter(beanClass, field);
		}
		else
		{
			accessors[1] = null;
		}

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

	public void setValueToField(Object target, Method setter, Object... args)
	{
		if (target != null)
		{

			try
			{
				setter.invoke(target, args);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	public <ID> ID getKey(Object entity, PropertyMeta<Void, ID> idMeta)
	{

		if (entity != null)
		{
			try
			{
				Object value = idMeta.getGetter().invoke(entity);
				return idMeta.getValue(value);
			}
			catch (Exception e)
			{
				throw new AchillesException("Cannot get primary key value by invoking getter '"
						+ idMeta.getGetter().getName() + "' from entity '" + entity + "'");
			}
		}
		return null;
	}

	public Long findSerialVersionUID(Class<?> entity)
	{

		log.trace("Find SerialVersionUID for entity {}", entity.getCanonicalName());

		Long serialVersionUID = null;
		try
		{
			Field declaredSerialVersionUID = entity.getDeclaredField("serialVersionUID");
			declaredSerialVersionUID.setAccessible(true);
			serialVersionUID = declaredSerialVersionUID.getLong(null);

		}
		catch (NoSuchFieldException e)
		{
			throw new BeanMappingException(
					"The 'serialVersionUID' property should be declared for entity '"
							+ entity.getCanonicalName() + "'", e);
		}
		catch (IllegalAccessException e)
		{
			throw new BeanMappingException(
					"The 'serialVersionUID' property should be publicly accessible for entity '"
							+ entity.getCanonicalName() + "'", e);
		}
		return serialVersionUID;
	}

	public String inferColumnFamilyName(Class<?> entity, String canonicalName)
	{
		log.debug("Infer column family name for entity {}", entity.getCanonicalName());

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
			throw new BeanMappingException("The entity '" + entity.getCanonicalName()
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

	public boolean isProxy(Object entity)
	{
		return Factory.class.isAssignableFrom(entity.getClass());
	}

	public Class<?> deriveBaseClass(Object entity)
	{
		Class<?> baseClass = entity.getClass();
		if (isProxy(entity))
		{
			Factory proxy = (Factory) entity;
			AchillesInterceptor interceptor = (AchillesInterceptor) proxy.getCallback(0);
			baseClass = interceptor.getTarget().getClass();
		}

		return baseClass;
	}

	public Object determinePrimaryKey(Object entity, EntityMeta<?> entityMeta)
	{
		log.trace("Determine primary key field for entity {}", entity.getClass().getCanonicalName());

		Object key;
		try
		{
			key = entityMeta.getIdMeta().getGetter().invoke(entity);
		}
		catch (Exception e)
		{
			key = null;
		}
		return key;
	}

	public List<Object> determineMultiKey(Object entity, List<Method> componentGetters)
	{
		List<Object> multiKeyValues = new ArrayList<Object>();

		if (entity != null)
		{

			for (Method getter : componentGetters)
			{

				Object key = null;
				try
				{
					key = getter.invoke(entity);
				}
				catch (Exception e)
				{
					throw new AchillesException("Cannot invoke getter '" + getter.getName()
							+ "' from entity '" + entity + "'", e);
				}
				multiKeyValues.add(key);
			}
		}
		return multiKeyValues;
	}
}
