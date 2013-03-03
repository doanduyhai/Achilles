package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.PropertyHelper.allowedCounterTypes;
import static info.archinnov.achilles.entity.PropertyHelper.allowedTypes;
import static info.archinnov.achilles.entity.PropertyHelper.isSupportedType;
import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.factory.PropertyMetaFactory.factory;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.MultiKey;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.JoinColumn;

import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * PropertyParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyParser
{

	private PropertyHelper propertyHelper = new PropertyHelper();
	private EntityHelper entityHelper = new EntityHelper();

	public PropertyMeta<?, ?> parse( //
			Map<String, PropertyMeta<?, ?>> propertyMetas, //
			Map<Field, String> externalWideMaps, //
			Class<?> entityClass, Field field, //
			boolean joinColumn, //
			ObjectMapper objectMapper, CounterDao counterDao)
	{
		String externalTableName;
		String propertyName;
		if (joinColumn)
		{
			JoinColumn column = field.getAnnotation(JoinColumn.class);
			externalTableName = field.getAnnotation(JoinColumn.class).table();
			propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
		}
		else
		{
			Column column = field.getAnnotation(Column.class);
			externalTableName = field.getAnnotation(Column.class).table();
			propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
		}
		boolean isWideMap = WideMap.class.isAssignableFrom(field.getType());

		Validator.validateFalse(propertyMetas.containsKey(propertyName),
				"The property '" + propertyName + "' is already used for the entity '"
						+ entityClass.getCanonicalName() + "'");

		PropertyMeta<?, ?> propertyMeta = null;
		if (StringUtils.isNotBlank(externalTableName) && isWideMap)
		{
			externalWideMaps.put(field, propertyName);
		}
		else
		{
			Class<?> fieldType = field.getType();

			if (List.class.isAssignableFrom(fieldType))
			{
				propertyMeta = parseListProperty(entityClass, field, propertyName, objectMapper);
			}

			else if (Set.class.isAssignableFrom(fieldType))
			{
				propertyMeta = parseSetProperty(entityClass, field, propertyName, objectMapper);
			}

			else if (Map.class.isAssignableFrom(fieldType))
			{
				propertyMeta = parseMapProperty(entityClass, field, propertyName, objectMapper);
			}

			else if (WideMap.class.isAssignableFrom(fieldType))
			{
				propertyMeta = parseWideMapProperty(entityClass, field, propertyName, objectMapper);
			}

			else
			{
				propertyMeta = parseSimpleProperty(entityClass, field, propertyName, objectMapper,
						entityClass.getCanonicalName(), counterDao);
			}

			propertyMetas.put(propertyName, propertyMeta);
		}

		return propertyMeta;
	}

	public PropertyMeta<Void, ?> parseSimpleProperty(Class<?> beanClass, Field field,
			String propertyName, ObjectMapper objectMapper, String fqcn, CounterDao counterDao)
	{
		Validator.validateSerializable(field.getType(), "Value of '" + field.getName()
				+ "' should be Serializable");

		Method[] accessors = entityHelper.findAccessors(beanClass, field);

		PropertyType type;
		CounterProperties counterProperties = null;
		if (propertyHelper.hasCounterAnnotation(field))
		{
			Validator
					.validateAllowedTypes(
							field.getType(),
							allowedCounterTypes,
							"Wrong type for the field '"
									+ field.getName()
									+ "'. Only java.lang.Long and primitive long are allowed for @Counter types");
			type = PropertyType.COUNTER;
			counterProperties = new CounterProperties(fqcn, counterDao);
		}
		else
		{
			type = propertyHelper.isLazy(field) ? LAZY_SIMPLE : SIMPLE;
		}

		return factory(field.getType()) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.accessors(accessors) //
				.counterProperties(counterProperties) //
				.build();

	}

	public PropertyMeta<Void, ?> parseListProperty(Class<?> beanClass, Field field,
			String propertyName, ObjectMapper objectMapper)
	{

		Class<?> valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);

		Validator.validateSerializable(valueClass, "List value type of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_LIST : LIST;

		return factory(valueClass) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.accessors(accessors).build();

	}

	public PropertyMeta<Void, ?> parseSetProperty(Class<?> beanClass, Field field,
			String propertyName, ObjectMapper objectMapper)
	{
		Class<?> valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);
		Validator.validateSerializable(valueClass, "Set value type of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_SET : SET;

		return factory(valueClass) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.accessors(accessors).build();
	}

	public PropertyMeta<?, ?> parseMapProperty(Class<?> beanClass, Field field,
			String propertyName, ObjectMapper objectMapper)
	{

		Class<?> valueClass;
		Class<?> keyType;

		Type genericType = field.getGenericType();

		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 1)
			{
				keyType = (Class<?>) actualTypeArguments[0];
				valueClass = (Class<?>) actualTypeArguments[1];
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
		Validator.validateSerializable(valueClass, "Map value type of '" + field.getName()
				+ "' should be Serializable");
		Validator.validateSerializable(keyType, "Map key type of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_MAP : MAP;

		return factory(keyType, valueClass) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.accessors(accessors).build();

	}

	public PropertyMeta<?, ?> parseWideMapProperty(Class<?> beanClass, Field field,
			String propertyName, ObjectMapper objectMapper)
	{

		PropertyType type = PropertyType.WIDE_MAP;
		MultiKeyProperties multiKeyProperties = null;
		Class<?> keyClass = null;
		Class<?> valueClass = null;

		// Multi or Single Key
		Type genericType = field.getGenericType();
		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 1)
			{
				keyClass = (Class<?>) actualTypeArguments[0];
				valueClass = (Class<?>) actualTypeArguments[1];

				if (MultiKey.class.isAssignableFrom(keyClass))
				{
					multiKeyProperties = propertyHelper.parseMultiKey(keyClass);
				}
				else
				{
					Validator
							.validateAllowedTypes(
									keyClass,
									allowedTypes,
									"The class '"
											+ keyClass.getCanonicalName()
											+ "' is not allowed as WideMap key. Did you forget to implement MultiKey interface ?");
				}
			}
			else
			{
				throw new BeanMappingException(
						"The WideMap type should be parameterized with <K,V> for the entity "
								+ beanClass.getCanonicalName());
			}
		}
		else
		{
			throw new BeanMappingException(
					"The WideMap type should be parameterized for the entity "
							+ beanClass.getCanonicalName());
		}

		Validator.validateSerializable(valueClass, "Wide map value of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);

		return factory(keyClass, valueClass) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.accessors(accessors) //
				.multiKeyProperties(multiKeyProperties) //
				.build();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <ID> PropertyMeta<?, ?> parseExternalWideMapProperty(Keyspace keyspace,
			PropertyMeta<Void, ID> idMeta, Class<?> entityClass, Field field, String propertyName,
			ObjectMapper objectMapper)
	{
		String externalColumnFamilyName = field.getAnnotation(Column.class).table();
		PropertyMeta<?, ?> propertyMeta = this.parseWideMapProperty(entityClass, field,
				propertyName, objectMapper);
		propertyMeta.setType(EXTERNAL_WIDE_MAP);
		GenericCompositeDao<ID, ?> dao;
		if (isSupportedType(propertyMeta.getValueClass()))
		{
			dao = new GenericCompositeDao(keyspace, idMeta.getValueSerializer(),
					propertyMeta.getValueSerializer(), externalColumnFamilyName);
		}
		else
		{
			dao = new GenericCompositeDao<ID, String>(keyspace, idMeta.getValueSerializer(),
					STRING_SRZ, externalColumnFamilyName);
		}

		propertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(
				externalColumnFamilyName, dao, idMeta.getValueSerializer()));

		return propertyMeta;
	}
}
