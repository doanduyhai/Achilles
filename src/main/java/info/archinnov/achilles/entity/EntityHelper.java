package info.archinnov.achilles.entity;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.PropertyFilter;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptorBuilder;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.persistence.Table;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	protected String[] deriveGetterName(Field field)
	{

		String camelCase = field.getName().substring(0, 1).toUpperCase()
				+ field.getName().substring(1);

		String[] getters;

		if (StringUtils.equals(field.getType().toString(), "boolean"))
		{
			getters = new String[]
			{
					"is" + camelCase,
					"get" + camelCase
			};
		}
		else
		{
			getters = new String[]
			{
				"get" + camelCase
			};
		}
		return getters;
	}

	protected String deriveSetterName(String fieldName)
	{

		return "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
	}

	public Method findGetter(Class<?> beanClass, Field field)
	{
		log.trace("Find getter for field {} in bean {}", field.getName(),
				beanClass.getCanonicalName());

		Method getterMethod = null;
		String fieldName = field.getName();
		String[] getters = this.deriveGetterName(field);

		for (String getter : getters)
		{
			try
			{
				getterMethod = beanClass.getMethod(getter);
				if (getterMethod.getReturnType() != field.getType())
				{
					throw new BeanMappingException("The getter for field '" + fieldName
							+ "' does not return correct type");
				}
			}
			catch (NoSuchMethodException e)
			{
				// Do nothing here
			}
		}
		if (getterMethod == null)
		{
			throw new BeanMappingException("The getter for field '" + fieldName
					+ "' does not exist");
		}
		return getterMethod;
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
		if (field.getType() != WideMap.class)
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
				throw new AchillesException("Cannot invoke '" + getter.getName() + "' on object '"
						+ target + "'", e);
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
				throw new AchillesException("Cannot invoke '" + setter.getName() + "' on object '"
						+ target + "'", e);
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
				return idMeta.castValue(value);
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
		if (table != null)
		{
			if (StringUtils.isNotBlank(table.name()))
			{
				return table.name();
			}
		}
		return canonicalName;
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
			AchillesInterceptor interceptor = this.getInterceptor(entity);
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

	@SuppressWarnings("unchecked")
	public <T> T buildProxy(T entity, EntityMeta<?> entityMeta)
	{
		if (entity == null)
		{
			return null;
		}

		Validator.validateNotNull(entityMeta, "entityMeta for proxy builder should not be");

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());

		enhancer.setCallback(JpaEntityInterceptorBuilder.builder(entityMeta, entity).build());

		return (T) enhancer.create();
	}

	@SuppressWarnings("unchecked")
	public <T, ID> T getRealObject(T proxy)
	{
		Factory factory = (Factory) proxy;
		JpaEntityInterceptor<ID, T> interceptor = (JpaEntityInterceptor<ID, T>) factory
				.getCallback(0);
		return (T) interceptor.getTarget();
	}

	@SuppressWarnings("unchecked")
	public <T, ID> JpaEntityInterceptor<ID, T> getInterceptor(T proxy)
	{
		Factory factory = (Factory) proxy;
		JpaEntityInterceptor<ID, T> interceptor = (JpaEntityInterceptor<ID, T>) factory
				.getCallback(0);
		return interceptor;
	}

	public <T> void ensureProxy(T proxy)
	{
		if (!this.isProxy(proxy))
		{
			throw new IllegalStateException("The entity '" + proxy + "' is not in 'managed' state.");
		}
	}

	public <T> T unproxy(T proxy)
	{
		if (this.isProxy(proxy))
		{
			return this.getRealObject(proxy);
		}
		else
		{
			return proxy;
		}
	}

	public <K, V> Entry<K, V> unproxy(Entry<K, V> entry)
	{
		V value = entry.getValue();
		if (this.isProxy(value))
		{
			value = this.getRealObject(value);
			entry.setValue(value);
		}
		return entry;
	}

	public <T> Collection<T> unproxy(Collection<T> proxies)
	{

		Collection<T> result = new ArrayList<T>();
		for (T proxy : proxies)
		{
			result.add(this.unproxy(proxy));
		}
		return result;
	}

	public <T> List<T> unproxy(List<T> proxies)
	{
		List<T> result = new ArrayList<T>();
		for (T proxy : proxies)
		{
			result.add(this.unproxy(proxy));
		}

		return result;
	}

	public <T> Set<T> unproxy(Set<T> proxies)
	{
		Set<T> result = new HashSet<T>();
		for (T proxy : proxies)
		{
			result.add(this.unproxy(proxy));
		}

		return result;
	}
}
