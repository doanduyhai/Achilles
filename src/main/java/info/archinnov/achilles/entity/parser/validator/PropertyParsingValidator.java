package info.archinnov.achilles.entity.parser.validator;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.ANY;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.parser.context.PropertyParsingContext;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.MultiKey;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

/**
 * PropertyParsingValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyParsingValidator
{

	public void validateNoDuplicate(PropertyParsingContext context)
	{
		String propertyName = context.getCurrentPropertyName();

		Validator.validateBeanMappingFalse(context.getPropertyMetas().containsKey(propertyName),
				"The property '" + propertyName + "' is already used for the entity '"
						+ context.getCurrentEntityClass().getCanonicalName() + "'");

	}

	public void validateCounterNotExternal(PropertyParsingContext context)
	{
		if (context.isExternal() && context.isCounterType())
		{
			throw new AchillesBeanMappingException(
					"Error for field '"
							+ context.getCurrentField().getName()
							+ "' of entity '"
							+ context.getCurrentEntityClass().getCanonicalName()
							+ "'. Counter value are already stored in external column families. There is no sense having a counter with external table");
		}
	}

	public void validateDirectCFMappingNoExternalWideMap(PropertyParsingContext context)
	{
		if (context.isExternal() && context.isColumnFamilyDirectMapping())
		{
			throw new AchillesBeanMappingException(
					"Error for field '"
							+ context.getCurrentField().getName()
							+ "' of entity '"
							+ context.getCurrentEntityClass().getCanonicalName()
							+ "'. Direct Column Family mapping cannot have external WideMap. It does not make sense");
		}
	}

	public void validateMapGenerics(Field field, Class<?> entityClass)
	{
		Type genericType = field.getGenericType();
		if (!(genericType instanceof ParameterizedType))
		{
			throw new AchillesBeanMappingException("The Map type should be parameterized for the entity "
					+ entityClass.getCanonicalName());
		}
		else
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length <= 1)
			{
				throw new AchillesBeanMappingException(
						"The Map type should be parameterized with <K,V> for the entity "
								+ entityClass.getCanonicalName());
			}
		}
	}

	public void validateWideMapGenerics(PropertyParsingContext context)
	{
		Type genericType = context.getCurrentField().getGenericType();
		Class<?> entityClass = context.getCurrentEntityClass();

		if (!(genericType instanceof ParameterizedType))
		{
			throw new AchillesBeanMappingException(
					"The WideMap type should be parameterized for the entity "
							+ entityClass.getCanonicalName());
		}
		else
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length <= 1)
			{
				throw new AchillesBeanMappingException(
						"The WideMap type should be parameterized with <K,V> for the entity "
								+ entityClass.getCanonicalName());
			}
		}
	}

	public void validateConsistencyLevelForCounter(PropertyParsingContext context,
			Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		if (consistencyLevels.left == ANY || consistencyLevels.right == ANY)
		{
			throw new AchillesBeanMappingException(
					"Counter field '"
							+ context.getCurrentField().getName()
							+ "' of entity '"
							+ context.getCurrentEntityClass().getCanonicalName()
							+ "' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");
		}
	}

	public static void validateAllowedTypes(Class<?> type, Set<Class<?>> allowedTypes,
			String message)
	{
		if (!allowedTypes.contains(type) && !MultiKey.class.isAssignableFrom(type)
				&& !type.isEnum())
		{
			throw new AchillesBeanMappingException(message);
		}
	}

}
