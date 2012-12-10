package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.builder.ListMetaBuilder.listMetaBuilder;
import static fr.doan.achilles.entity.metadata.builder.MapMetaBuilder.mapMetaBuilder;
import static fr.doan.achilles.entity.metadata.builder.MultiKeyWideMapMetaBuilder.multiKeyWideMapPropertyMetaBuiler;
import static fr.doan.achilles.entity.metadata.builder.SetMetaBuilder.setMetaBuilder;
import static fr.doan.achilles.entity.metadata.builder.SimpleMetaBuilder.simpleMetaBuilder;
import static fr.doan.achilles.entity.metadata.builder.WideMapMetaBuilder.wideMapPropertyMetaBuiler;

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
import fr.doan.achilles.entity.EntityPropertyHelper;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.MultiKey;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.validation.Validator;

public class PropertyParser
{

	private Set<Class<?>> allowedTypes = new HashSet<Class<?>>();

	public PropertyParser() {
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

	private final EntityPropertyHelper helper = new EntityPropertyHelper();

	public <V> PropertyMeta<V> parse(Class<?> beanClass, Field field, String propertyName)
	{
		Class<?> fieldType = field.getType();

		PropertyMeta<V> propertyMeta;

		if (List.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseListProperty(beanClass, field, propertyName, fieldType);
		}

		else if (Set.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseSetProperty(beanClass, field, propertyName, fieldType);
		}

		else if (Map.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseMapProperty(beanClass, field, propertyName, fieldType);
		}

		else if (WideMap.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseWideMapProperty(beanClass, field, propertyName, fieldType);
		}

		else
		{
			propertyMeta = parseSimpleProperty(beanClass, field, propertyName);
		}
		return propertyMeta;
	}

	@SuppressWarnings("unchecked")
	private <V> PropertyMeta<V> parseSimpleProperty(Class<?> beanClass, Field field,
			String propertyName)
	{
		Validator.validateSerializable(field.getType(), "property '" + field.getName() + "'");
		Method[] accessors = helper.findAccessors(beanClass, field);
		boolean lazy = this.isLazy(field);
		return simpleMetaBuilder((Class<V>) field.getType()).propertyName(propertyName)
				.accessors(accessors).lazy(lazy).build();
	}

	@SuppressWarnings(
	{
		"unchecked",
	})
	private <V> PropertyMeta<V> parseListProperty(Class<?> beanClass, Field field,
			String propertyName, Class<?> fieldType)
	{

		Class<?> valueClass;
		Type genericType = field.getGenericType();

		valueClass = inferValueClass(genericType);

		Validator.validateSerializable(valueClass, "list value type of '" + field.getName() + "'");
		Method[] accessors = helper.findAccessors(beanClass, field);
		boolean lazy = this.isLazy(field);
		return listMetaBuilder((Class<V>) valueClass).propertyName(propertyName)
				.accessors(accessors).lazy(lazy).build();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private <V> PropertyMeta<V> parseSetProperty(Class<?> beanClass, Field field,
			String propertyName, Class<?> fieldType)
	{

		Class valueClass;
		Type genericType = field.getGenericType();

		valueClass = inferValueClass(genericType);
		Validator.validateSerializable(valueClass, "set value type of '" + field.getName() + "'");
		Method[] accessors = helper.findAccessors(beanClass, field);
		boolean lazy = this.isLazy(field);
		return setMetaBuilder((Class<V>) valueClass).propertyName(propertyName)
				.accessors(accessors).lazy(lazy).build();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private <V> PropertyMeta<V> parseMapProperty(Class<?> beanClass, Field field,
			String propertyName, Class<?> fieldType)
	{

		Class valueClass;
		Class keyType;

		Type genericType = field.getGenericType();

		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 1)
			{
				keyType = (Class) actualTypeArguments[0];
				valueClass = (Class) actualTypeArguments[1];
			}
			else
			{
				keyType = Object.class;
				valueClass = Object.class;
			}
		}
		else
		{
			keyType = Object.class;
			valueClass = Object.class;
		}
		Validator.validateSerializable(valueClass, "map value type of '" + field.getName() + "'");
		Validator.validateSerializable(keyType, "map key type of '" + field.getName() + "'");
		Method[] accessors = helper.findAccessors(beanClass, field);
		boolean lazy = this.isLazy(field);
		return mapMetaBuilder(keyType, valueClass).propertyName(propertyName).accessors(accessors)
				.lazy(lazy).build();
	}

	@SuppressWarnings("unchecked")
	private <V> PropertyMeta<V> parseWideMapProperty(Class<?> beanClass, Field field,
			String propertyName, Class<?> fieldType)
	{
		List<Class<?>> componentClasses = new ArrayList<Class<?>>();
		List<Method> componentGetters = new ArrayList<Method>();
		List<Method> componentSetters = new ArrayList<Method>();

		Class<?> keyClass;
		Class<V> valueClass;

		Type genericType = field.getGenericType();
		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 1)
			{
				keyClass = (Class<?>) actualTypeArguments[0];
				valueClass = (Class<V>) actualTypeArguments[1];

				if (MultiKey.class.isAssignableFrom(keyClass))
				{
					parseMultiKey(componentClasses, componentGetters, componentSetters, keyClass);
				}
				else
				{
					Validator.validateAllowedTypes(keyClass, allowedTypes,
							"The class '" + keyClass.getCanonicalName()
									+ "' is not allowed as WideMap key");
				}
			}
			else
			{
				throw new IncorrectTypeException(
						"The WideMap type should be parameterized with <K,V> for the entity "
								+ beanClass.getCanonicalName());
			}
		}
		else
		{
			throw new IncorrectTypeException(
					"The WideMap type should be parameterized for the entity "
							+ beanClass.getCanonicalName());
		}

		Validator.validateSerializable(valueClass, "value type of " + field.getName());
		Method[] accessors = helper.findAccessors(beanClass, field);
		if (componentClasses.size() == 0)
		{
			return wideMapPropertyMetaBuiler(keyClass, valueClass).propertyName(propertyName)
					.accessors(accessors).build();
		}
		else
		{
			return multiKeyWideMapPropertyMetaBuiler(keyClass, valueClass)
					.componentClasses(componentClasses) //
					.componentGetters(componentGetters) //
					.componentSetters(componentSetters) //
					.propertyName(propertyName).accessors(accessors).build();
		}

	}

	private void parseMultiKey(List<Class<?>> componentClasses, List<Method> componentGetters,
			List<Method> componentSetters, Class<?> keyClass)
	{
		int keySum = 0;
		int keyCount = 0;
		Map<Integer, Field> components = new HashMap<Integer, Field>();

		for (Field multiKeyField : keyClass.getDeclaredFields())
		{
			Key keyAnnotation = multiKeyField.getAnnotation(Key.class);
			if (keyAnnotation != null)
			{
				keyCount++;
				keySum += keyAnnotation.order();

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
			componentGetters.add(helper.findGetter(keyClass, multiKeyField));
			componentSetters.add(helper.findSetter(keyClass, multiKeyField));
			componentClasses.add(multiKeyField.getType());
		}

		Validator.validateNotEmpty(componentClasses,
				"No field with @Key annotation found in the class '" + keyClass.getCanonicalName()
						+ "'");
		Validator.validateInstantiable(keyClass);
	}

	private Class<?> inferValueClass(Type genericType)
	{
		Class<?> valueClass;
		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 0)
			{
				valueClass = (Class<?>) actualTypeArguments[0];
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

	private boolean isLazy(Field field)
	{
		boolean lazy = false;
		if (field.getAnnotation(Lazy.class) != null)
		{
			lazy = true;
		}
		return lazy;
	}
}
