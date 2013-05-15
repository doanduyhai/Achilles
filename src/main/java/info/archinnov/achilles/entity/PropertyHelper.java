package info.archinnov.achilles.entity;

import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.parser.validator.PropertyParsingValidator;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.validation.Validator;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
//import me.prettyprint.hector.api.Serializer;
//import me.prettyprint.hector.api.beans.AbstractComposite.Component;

/**
 * PropertyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyHelper
{
	private static final Logger log = LoggerFactory.getLogger(PropertyHelper.class);

	public static Set<Class<?>> allowedTypes = new HashSet<Class<?>>();
	protected AchillesEntityIntrospector achillesEntityIntrospector = new AchillesEntityIntrospector();

	static
	{
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

	public PropertyHelper() {}

	public MultiKeyProperties parseMultiKey(Class<?> keyClass)
	{
		log.debug("Parse multikey class {} ", keyClass.getCanonicalName());

		List<Class<?>> componentClasses = new ArrayList<Class<?>>();
		List<Method> componentGetters = new ArrayList<Method>();
		List<Method> componentSetters = new ArrayList<Method>();

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
					throw new AchillesBeanMappingException("The order '" + order
							+ "' is duplicated in MultiKey class '" + keyClass.getCanonicalName()
							+ "'");
				}
				else
				{
					orders.add(order);
				}
				keySum += order;

				Class<?> keySubType = multiKeyField.getType();
				PropertyParsingValidator.validateAllowedTypes(
						keySubType,
						allowedTypes,
						"The class '" + keySubType.getCanonicalName()
								+ "' is not a valid key type for the MultiKey class '"
								+ keyClass.getCanonicalName() + "'");

				components.put(keyAnnotation.order(), multiKeyField);

			}
		}

		int check = (keyCount * (keyCount + 1)) / 2;

		log.debug("Validate key ordering multikey class {} ", keyClass.getCanonicalName());

		Validator.validateBeanMappingTrue(keySum == check,
				"The key orders is wrong for MultiKey class '" + keyClass.getCanonicalName() + "'");

		List<Integer> orderList = new ArrayList<Integer>(components.keySet());
		Collections.sort(orderList);
		for (Integer order : orderList)
		{
			Field multiKeyField = components.get(order);
			componentGetters.add(achillesEntityIntrospector.findGetter(keyClass, multiKeyField));
			componentSetters.add(achillesEntityIntrospector.findSetter(keyClass, multiKeyField));
			componentClasses.add(multiKeyField.getType());
		}

		Validator.validateBeanMappingNotEmpty(componentClasses,
				"No field with @Key annotation found in the class '" + keyClass.getCanonicalName()
						+ "'");
		Validator.validateInstantiable(keyClass);

		MultiKeyProperties multiKeyProperties = new MultiKeyProperties();
		multiKeyProperties.setComponentClasses(componentClasses);
		multiKeyProperties.setComponentGetters(componentGetters);
		multiKeyProperties.setComponentSetters(componentSetters);

		log.trace("Built multi key properties : {}", multiKeyProperties);
		return multiKeyProperties;
	}

	@SuppressWarnings("unchecked")
	public <T> Class<T> inferValueClassForListOrSet(Type genericType, Class<?> entityClass)
	{
		log.debug("Infer parameterized value class for collection type {} of entity class {} ",
				genericType.toString(), entityClass.getCanonicalName());

		Class<T> valueClass;
		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 0)
			{
				valueClass = (Class<T>) actualTypeArguments[actualTypeArguments.length - 1];
			}
			else
			{
				throw new AchillesBeanMappingException("The type '"
						+ genericType.getClass().getCanonicalName() + "' of the entity '"
						+ entityClass.getCanonicalName() + "' should be parameterized");
			}
		}
		else
		{
			throw new AchillesBeanMappingException("The type '"
					+ genericType.getClass().getCanonicalName() + "' of the entity '"
					+ entityClass.getCanonicalName() + "' should be parameterized");
		}

		log.trace("Inferred value class : {}", valueClass.getCanonicalName());

		return valueClass;
	}

	public boolean isLazy(Field field)
	{
		log.debug("Check @Lazy annotation on field {} of class {}", field.getName(), field
				.getDeclaringClass().getCanonicalName());

		boolean lazy = false;
		if (field.getAnnotation(Lazy.class) != null)
		{
			lazy = true;
		}
		return lazy;
	}

	public boolean hasConsistencyAnnotation(Field field)
	{
		log.debug("Check @Consistency annotation on field {} of class {}", field.getName(), field
				.getDeclaringClass().getCanonicalName());

		boolean consistency = false;
		if (field.getAnnotation(Consistency.class) != null)
		{
			consistency = true;
		}
		return consistency;
	}

	public static <T> boolean isSupportedType(Class<T> valueClass)
	{
		return allowedTypes.contains(valueClass);
	}

	public <T> Pair<ConsistencyLevel, ConsistencyLevel> findConsistencyLevels(Field field,
			AchillesConsistencyLevelPolicy policy)
	{
		log.debug("Find consistency configuration for field {} of class {}", field.getName(), field
				.getDeclaringClass().getCanonicalName());

		Consistency clevel = field.getAnnotation(Consistency.class);

		ConsistencyLevel defaultGlobalRead = achillesEntityIntrospector
				.getDefaultGlobalReadConsistency(policy);
		ConsistencyLevel defaultGlobalWrite = achillesEntityIntrospector
				.getDefaultGlobalWriteConsistency(policy);

		if (clevel != null)
		{
			defaultGlobalRead = clevel.read();
			defaultGlobalWrite = clevel.write();
		}

		log.trace("Found consistency levels : {} / {}", defaultGlobalRead, defaultGlobalWrite);
		return new Pair<ConsistencyLevel, ConsistencyLevel>(defaultGlobalRead, defaultGlobalWrite);
	}
}
