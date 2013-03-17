package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.PropertyHelper.allowedCounterTypes;
import static info.archinnov.achilles.entity.PropertyHelper.allowedTypes;
import static info.archinnov.achilles.entity.PropertyHelper.isSupportedType;
import static info.archinnov.achilles.entity.manager.ThriftEntityManagerFactoryImpl.configurableCLPolicyTL;
import static info.archinnov.achilles.entity.manager.ThriftEntityManagerFactoryImpl.counterDaoTL;
import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_WIDE_MAP_COUNTER;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.factory.PropertyMetaFactory.factory;
import static info.archinnov.achilles.entity.parser.EntityParser.consistencyLevelsTL;
import static info.archinnov.achilles.entity.parser.EntityParser.counterMetasTL;
import static info.archinnov.achilles.entity.parser.EntityParser.entityClassTL;
import static info.archinnov.achilles.entity.parser.EntityParser.externalWideMapTL;
import static info.archinnov.achilles.entity.parser.EntityParser.objectMapperTL;
import static info.archinnov.achilles.entity.parser.EntityParser.propertyMetasTL;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ANY;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactoryImpl;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
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

import me.prettyprint.hector.api.HConsistencyLevel;
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

	public PropertyMeta<?, ?> parse(Field field, boolean joinColumn)
	{

		Class<?> entityClass = entityClassTL.get();

		String[] result = this.inferPropertyNameAndExternalTableName(field, joinColumn);
		String propertyName = result[0];
		String externalTableName = result[1];

		boolean isCounter = propertyHelper.hasCounterAnnotation(field);
		boolean isCustomConsistencyLevel = propertyHelper.hasConsistencyAnnotation(field);
		boolean isExternal = StringUtils.isNotBlank(externalTableName);

		Validator.validateBeanMappingFalse(propertyMetasTL.get().containsKey(propertyName),
				"The property '" + propertyName + "' is already used for the entity '"
						+ entityClass.getCanonicalName() + "'");

		PropertyMeta<?, ?> propertyMeta = null;
		if (isExternal && isCounter)
		{
			throw new BeanMappingException(
					"Error for field '"
							+ field.getName()
							+ "' of entity '"
							+ entityClass.getCanonicalName()
							+ "'. Counter value are already stored in external column families. There is no sense having a counter with external table");
		}
		else
		{
			Class<?> fieldType = field.getType();

			if (List.class.isAssignableFrom(fieldType))
			{
				propertyMeta = parseListProperty(field, propertyName);
			}

			else if (Set.class.isAssignableFrom(fieldType))
			{
				propertyMeta = parseSetProperty(field, propertyName);
			}

			else if (Map.class.isAssignableFrom(fieldType))
			{
				propertyMeta = parseMapProperty(field, propertyName);
			}

			else if (WideMap.class.isAssignableFrom(fieldType))
			{
				propertyMeta = parseWideMapProperty(field, propertyName,
						entityClass.getCanonicalName());
			}

			else
			{
				propertyMeta = parseSimpleProperty(field, propertyName,
						entityClass.getCanonicalName());
			}

			propertyMetasTL.get().put(propertyName, propertyMeta);
			if (isExternal)
			{
				processExternalWideMap(field, externalTableName, isCustomConsistencyLevel,
						isExternal, propertyMeta);
			}
			if (isCounter && isCustomConsistencyLevel)
			{
				processCounterConsistencyLevel(field, entityClass, isCounter,
						isCustomConsistencyLevel, propertyMeta);
			}
		}

		return propertyMeta;
	}

	public PropertyMeta<Void, ?> parseSimpleProperty(Field field, String propertyName, String fqcn)
	{
		Validator.validateSerializable(field.getType(), "Value of '" + field.getName()
				+ "' should be Serializable");
		ObjectMapper objectMapper = objectMapperTL.get();
		Class<?> entityClass = entityClassTL.get();
		Method[] accessors = entityHelper.findAccessors(entityClass, field);
		PropertyType type;
		CounterProperties counterProperties = null;
		if (propertyHelper.hasCounterAnnotation(field))
		{
			counterProperties = buildCounterProperties(field.getType(), field.getName(), fqcn);
			type = PropertyType.COUNTER;
		}
		else
		{
			type = propertyHelper.isLazy(field) ? LAZY_SIMPLE : SIMPLE;
		}

		PropertyMeta<Void, ?> propertyMeta = factory(field.getType()) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.accessors(accessors) //
				.counterProperties(counterProperties) //
				.consistencyLevels(consistencyLevelsTL.get()) //
				.build();

		if (counterProperties != null)
		{
			counterMetasTL.get().add(propertyMeta);
		}

		return propertyMeta;

	}

	public PropertyMeta<Void, ?> parseListProperty(Field field, String propertyName)
	{

		ObjectMapper objectMapper = objectMapperTL.get();
		Class<?> entityClass = entityClassTL.get();

		Class<?> valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);

		Validator.validateSerializable(valueClass, "List value type of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(entityClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_LIST : LIST;

		return factory(valueClass) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.consistencyLevels(consistencyLevelsTL.get()) //
				.accessors(accessors).build();

	}

	public PropertyMeta<Void, ?> parseSetProperty(Field field, String propertyName)
	{
		ObjectMapper objectMapper = objectMapperTL.get();
		Class<?> entityClass = entityClassTL.get();

		Class<?> valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);
		Validator.validateSerializable(valueClass, "Set value type of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(entityClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_SET : SET;

		return factory(valueClass) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.consistencyLevels(consistencyLevelsTL.get()) //
				.accessors(accessors).build();
	}

	public PropertyMeta<?, ?> parseMapProperty(Field field, String propertyName)
	{

		ObjectMapper objectMapper = objectMapperTL.get();
		Class<?> entityClass = entityClassTL.get();

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
		Method[] accessors = entityHelper.findAccessors(entityClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_MAP : MAP;

		return factory(keyType, valueClass) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.consistencyLevels(consistencyLevelsTL.get()) //
				.accessors(accessors).build();

	}

	public PropertyMeta<?, ?> parseWideMapProperty(Field field, String propertyName, String fqcn)
	{

		ObjectMapper objectMapper = objectMapperTL.get();
		Class<?> entityClass = entityClassTL.get();

		PropertyType type = PropertyType.WIDE_MAP;
		MultiKeyProperties multiKeyProperties = null;
		CounterProperties counterProperties = null;

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

				multiKeyProperties = parseWideMapKey(multiKeyProperties, keyClass);
				if (propertyHelper.hasCounterAnnotation(field))
				{
					counterProperties = buildCounterProperties(valueClass, field.getName(), fqcn);
					type = EXTERNAL_WIDE_MAP_COUNTER;
				}
			}
			else
			{
				throw new BeanMappingException(
						"The WideMap type should be parameterized with <K,V> for the entity "
								+ entityClass.getCanonicalName());
			}
		}
		else
		{
			throw new BeanMappingException(
					"The WideMap type should be parameterized for the entity "
							+ entityClass.getCanonicalName());
		}

		Validator.validateSerializable(valueClass, "Wide map value of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(entityClass, field);

		PropertyMeta<?, ?> propertyMeta = factory(keyClass, valueClass) //
				.objectMapper(objectMapper) //
				.type(type) //
				.propertyName(propertyName) //
				.accessors(accessors) //
				.multiKeyProperties(multiKeyProperties) //
				.counterProperties(counterProperties) //
				.consistencyLevels(consistencyLevelsTL.get()) //
				.build();

		if (counterProperties != null)
		{
			counterMetasTL.get().add(propertyMeta);
		}

		return propertyMeta;
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <ID> void fillExternalWideMap(Keyspace keyspace, PropertyMeta<Void, ID> idMeta,
			PropertyMeta<?, ?> propertyMeta, String externalTableName)
	{
		propertyMeta.setType(EXTERNAL_WIDE_MAP);
		GenericCompositeDao<ID, ?> dao;
		if (isSupportedType(propertyMeta.getValueClass()))
		{
			dao = new GenericCompositeDao(keyspace, idMeta.getValueSerializer(),
					propertyMeta.getValueSerializer(), externalTableName,
					configurableCLPolicyTL.get());
		}
		else
		{
			dao = new GenericCompositeDao<ID, String>(keyspace, idMeta.getValueSerializer(),
					STRING_SRZ, externalTableName, configurableCLPolicyTL.get());
		}

		propertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(
				externalTableName, dao, idMeta.getValueSerializer()));

		propertyMetasTL.get().put(propertyMeta.getPropertyName(), propertyMeta);
	}

	private <T> CounterProperties buildCounterProperties(Class<T> valueClass, String fieldName,
			String fqcn)
	{
		CounterProperties counterProperties;
		Validator
				.validateAllowedTypes(
						valueClass,
						allowedCounterTypes,
						"Wrong counter type for the field '"
								+ fieldName
								+ "'. Only java.lang.Long and primitive long are allowed for @Counter types");
		counterProperties = new CounterProperties(fqcn, counterDaoTL.get());
		return counterProperties;
	}

	private MultiKeyProperties parseWideMapKey(MultiKeyProperties multiKeyProperties,
			Class<?> keyClass)
	{
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
		return multiKeyProperties;
	}

	private String[] inferPropertyNameAndExternalTableName(Field field, boolean joinColumn)
	{
		String propertyName, externalTableName;
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

		return new String[]
		{
				propertyName,
				externalTableName
		};
	}

	private void processExternalWideMap(Field field, String externalTableName,
			boolean isCustomConsistencyLevel, boolean isExternal, PropertyMeta<?, ?> propertyMeta)
	{

		externalWideMapTL.get().put(propertyMeta, externalTableName);

		if (isCustomConsistencyLevel)
		{
			Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> consistencyLevels = propertyHelper
					.findConsistencyLevels(field);
			ThriftEntityManagerFactoryImpl.configurableCLPolicyTL.get().setConsistencyLevelForRead(
					consistencyLevels.right.left, externalTableName);
			ThriftEntityManagerFactoryImpl.configurableCLPolicyTL.get()
					.setConsistencyLevelForWrite(consistencyLevels.right.right, externalTableName);
			propertyMeta.setConsistencyLevels(consistencyLevels.left);
		}
	}

	private void processCounterConsistencyLevel(Field field, Class<?> entityClass,
			boolean isCounter, boolean isCustomConsistencyLevel, PropertyMeta<?, ?> propertyMeta)
	{

		Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = propertyHelper
				.findConsistencyLevels(field).left;

		if (consistencyLevels.left == ANY || consistencyLevels.right == ANY)
		{
			throw new BeanMappingException(
					"Counter field '"
							+ field.getName()
							+ "' of entity '"
							+ entityClass.getCanonicalName()
							+ "' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");
		}
		propertyMeta.setConsistencyLevels(consistencyLevels);
	}
}
