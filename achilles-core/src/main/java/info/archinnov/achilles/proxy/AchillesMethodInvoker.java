package info.archinnov.achilles.proxy;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AchillesMethodInvoker
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesMethodInvoker
{
	private static final Logger log = LoggerFactory.getLogger(AchillesMethodInvoker.class);

	public List<Object> determineMultiKeyValues(Object entity, List<Method> componentGetters)
	{
		log.debug("Determine multi-key components for entity {} ", entity);

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
							+ "' of type '" + getter.getDeclaringClass().getCanonicalName()
							+ "' from entity '" + entity + "'", e);
				}
				multiKeyValues.add(key);
			}
		}
		log.trace("Found multi key values : {}", multiKeyValues);
		return multiKeyValues;
	}

	public Object getPrimaryKey(Object entity, PropertyMeta<?, ?> idMeta)
	{
		Method getter = idMeta.getGetter();

		log.trace("Get primary key {} from instance {} of class {}", idMeta.getPropertyName(),
				entity, getter.getDeclaringClass().getCanonicalName());

		if (entity != null)
		{
			try
			{
				Object value = getter.invoke(entity);
				return idMeta.castValue(value);
			}
			catch (Exception e)
			{
				throw new AchillesException("Cannot get primary key value by invoking getter '"
						+ getter.getName() + "' of type '"
						+ getter.getDeclaringClass().getCanonicalName() + "' from entity '"
						+ entity + "'");
			}
		}
		return null;
	}

	public Object getValueFromField(Object target, Method getter)
	{
		log.trace("Get value with getter {} from instance {} of class {}", getter.getName(),
				target, getter.getDeclaringClass().getCanonicalName());

		Object value = null;

		if (target != null)
		{

			try
			{
				value = getter.invoke(target);
			}
			catch (Exception e)
			{
				throw new AchillesException("Cannot invoke '" + getter.getName() + "' of type '"
						+ getter.getDeclaringClass().getCanonicalName() + "' on instance '"
						+ target + "'", e);
			}
		}

		log.trace("Found value : {}", value);
		return value;
	}

	public void setValueToField(Object target, Method setter, Object... args)
	{
		log.trace("Set value with setter {} to instance {} of class {} with {}", setter.getName(),
				target, setter.getDeclaringClass().getCanonicalName(), args);

		if (target != null)
		{
			try
			{
				setter.invoke(target, args);
			}
			catch (Exception e)
			{
				throw new AchillesException("Cannot invoke '" + setter.getName() + "'  of type '"
						+ setter.getDeclaringClass().getCanonicalName() + "' on instance '"
						+ target + "'", e);
			}
		}
	}
}
