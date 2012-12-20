package fr.doan.achilles.entity;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import fr.doan.achilles.annotations.Key;
import fr.doan.achilles.annotations.Lazy;
import fr.doan.achilles.exception.BeanMappingException;
import fr.doan.achilles.validation.Validator;

/**
 * PropertyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyHelper
{

	public Set<Class<?>> allowedTypes = new HashSet<Class<?>>();
	private final EntityHelper entityHelper = new EntityHelper();

	public PropertyHelper() {
		// Bytes
		allowedTypes.add(byte[].class);
		allowedTypes.add(ByteBuffer.class);

		// Boolean
		allowedTypes.add(Boolean.class);
		allowedTypes.add(boolean.class);

		// Date
		allowedTypes.add(Date.class);

		// Double
		allowedTypes.add(Double.class);
		allowedTypes.add(double.class);

		// Char
		allowedTypes.add(Character.class);

		// Char
		allowedTypes.add(Character.class);

		// Float
		allowedTypes.add(Float.class);
		allowedTypes.add(float.class);

		// Integer
		allowedTypes.add(BigInteger.class);
		allowedTypes.add(Integer.class);
		allowedTypes.add(int.class);

		// Long
		allowedTypes.add(Long.class);
		allowedTypes.add(long.class);

		// Short
		allowedTypes.add(Short.class);
		allowedTypes.add(short.class);

		// String
		allowedTypes.add(String.class);

		// UUID
		allowedTypes.add(UUID.class);

	}

	public void parseMultiKey(List<Class<?>> componentClasses, List<Method> componentGetters,
			List<Method> componentSetters, Class<?> keyClass)
	{
		int keySum = 0;
		int keyCount = 0;
		Map<Integer, Field> components = new HashMap<Integer, Field>();

		Set<Integer> orders = new HashSet<Integer>();
		for (Field multiKeyField : keyClass.getDeclaredFields())
		{
			Key keyAnnotation = multiKeyField.getAnnotation(Key.class);
			if (keyAnnotation != null)
			{
				keyCount++;
				int order = keyAnnotation.order();
				if (orders.contains(order))
				{
					throw new BeanMappingException("The order '" + order
							+ "' is duplicated in MultiKey '" + keyClass.getCanonicalName() + "'");
				}
				else
				{
					orders.add(order);
				}
				keySum += order;

				Class<?> keySubType = multiKeyField.getType();
				Validator.validateAllowedTypes(
						keySubType,
						allowedTypes,
						"The class '" + keySubType.getCanonicalName()
								+ "' is not a valid key type for the MultiKey class '"
								+ keyClass.getCanonicalName() + "'");

				components.put(keyAnnotation.order(), multiKeyField);

			}
		}

		int check = (keyCount * (keyCount + 1)) / 2;

		Validator.validateTrue(keySum == check, "The key orders is wrong for MultiKey class '"
				+ keyClass.getCanonicalName() + "'");

		List<Integer> orderList = new ArrayList<Integer>(components.keySet());
		Collections.sort(orderList);
		for (Integer order : orderList)
		{
			Field multiKeyField = components.get(order);
			componentGetters.add(entityHelper.findGetter(keyClass, multiKeyField));
			componentSetters.add(entityHelper.findSetter(keyClass, multiKeyField));
			componentClasses.add(multiKeyField.getType());
		}

		Validator.validateNotEmpty(componentClasses,
				"No field with @Key annotation found in the class '" + keyClass.getCanonicalName()
						+ "'");
		Validator.validateInstantiable(keyClass);
	}

	public Class<?> inferValueClass(Type genericType)
	{
		Class<?> valueClass;
		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 0)
			{
				valueClass = (Class<?>) actualTypeArguments[actualTypeArguments.length - 1];
			}
			else
			{
				valueClass = Object.class;
			}
		}
		else
		{
			valueClass = Object.class;
		}
		return valueClass;
	}

	public Class<?> inferKeyClass(Type genericType)
	{
		Class<?> keyClass;
		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 1)
			{
				keyClass = (Class<?>) actualTypeArguments[0];
			}
			else
			{
				keyClass = null;
			}
		}
		else
		{
			keyClass = null;
		}
		return keyClass;
	}

	public boolean isLazy(Field field)
	{
		boolean lazy = false;
		if (field.getAnnotation(Lazy.class) != null)
		{
			lazy = true;
		}
		return lazy;
	}
}
