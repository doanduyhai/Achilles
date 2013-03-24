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
import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP_COUNTER;
import static info.archinnov.achilles.entity.metadata.factory.PropertyMetaFactory.factory;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parser.validator.PropertyParsingValidator;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.MultiKey;
import info.archinnov.achilles.entity.type.WideMap;
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

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;

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
	private PropertyParsingValidator validator = new PropertyParsingValidator();

	public PropertyMeta<?, ?> parse(PropertyParsingContext context)
	{
		Field field = context.getCurrentField();
		this.inferPropertyNameAndExternalTableName(context);
		context.setCounterType(propertyHelper.hasCounterAnnotation(field));
		context.setCustomConsistencyLevels(propertyHelper.hasConsistencyAnnotation(context
				.getCurrentField()));

		this.validator.validateNoDuplicate(context);
		this.validator.validateCounterNotExternal(context);
		this.validator.validateDirectCFMappingNoExternalWideMap(context);

		Class<?> fieldType = field.getType();
		PropertyMeta<?, ?> propertyMeta;
		if (List.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseListProperty(context);
		}

		else if (Set.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseSetProperty(context);
		}

		else if (Map.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseMapProperty(context);
		}

		else if (WideMap.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseWideMapProperty(context);
		}
		else
		{
			propertyMeta = parseSimpleProperty(context);
		}

		if (!context.isPrimaryKey())
		{
			context.getPropertyMetas().put(context.getCurrentPropertyName(), propertyMeta);
		}
		return propertyMeta;
	}

	public PropertyMeta<Void, ?> parseSimpleProperty(PropertyParsingContext context)
	{

		Class<?> entityClass = context.getCurrentEntityClass();
		Field field = context.getCurrentField();

		Validator.validateSerializable(field.getType(), "Value of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(entityClass, field);
		PropertyType type;
		CounterProperties counterProperties = null;
		if (context.isCounterType())
		{
			counterProperties = buildCounterProperties(field.getType(), context);
			type = PropertyType.COUNTER;
		}
		else
		{
			type = propertyHelper.isLazy(field) ? LAZY_SIMPLE : SIMPLE;
		}

		PropertyMeta<Void, ?> propertyMeta = factory(field.getType()) //
				.objectMapper(context.getCurrentObjectMapper()) //
				.type(type) //
				.propertyName(context.getCurrentPropertyName()) //
				.accessors(accessors) //
				.counterProperties(counterProperties) //
				.consistencyLevels(context.getCurrentConsistencyLevels()) //
				.build();

		if (context.isCounterType())
		{
			context.getCounterMetas().add(propertyMeta);
			if (context.isCustomConsistencyLevels())
			{
				parseSimpleCounterConsistencyLevel(context, propertyMeta);
			}
		}

		return propertyMeta;

	}

	public PropertyMeta<Void, ?> parseListProperty(PropertyParsingContext context)
	{

		Class<?> entityClass = context.getCurrentEntityClass();
		Field field = context.getCurrentField();
		Class<?> valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);

		Validator.validateSerializable(valueClass, "List value type of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(entityClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_LIST : LIST;

		return factory(valueClass) //
				.objectMapper(context.getCurrentObjectMapper()) //
				.type(type) //
				.propertyName(context.getCurrentPropertyName()) //
				.consistencyLevels(context.getCurrentConsistencyLevels()) //
				.accessors(accessors).build();

	}

	public PropertyMeta<Void, ?> parseSetProperty(PropertyParsingContext context)
	{
		Class<?> entityClass = context.getCurrentEntityClass();
		Field field = context.getCurrentField();

		Class<?> valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);
		Validator.validateSerializable(valueClass, "Set value type of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(entityClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_SET : SET;

		return factory(valueClass) //
				.objectMapper(context.getCurrentObjectMapper()) //
				.type(type) //
				.propertyName(context.getCurrentPropertyName()) //
				.consistencyLevels(context.getCurrentConsistencyLevels()) //
				.accessors(accessors).build();
	}

	public PropertyMeta<?, ?> parseMapProperty(PropertyParsingContext context)
	{
		Class<?> entityClass = context.getCurrentEntityClass();
		Field field = context.getCurrentField();

		validator.validateMapGenerics(field, entityClass);

		Pair<Class<?>, Class<?>> types = determineMapGenericTypes(field);
		Class<?> keyClass = types.left;
		Class<?> valueClass = types.right;

		Validator.validateSerializable(valueClass, "Map value type of '" + field.getName()
				+ "' should be Serializable");
		Validator.validateSerializable(keyClass, "Map key type of '" + field.getName()
				+ "' should be Serializable");

		Method[] accessors = entityHelper.findAccessors(entityClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_MAP : MAP;

		return factory(keyClass, valueClass) //
				.objectMapper(context.getCurrentObjectMapper()) //
				.type(type) //
				.propertyName(context.getCurrentPropertyName()) //
				.consistencyLevels(context.getCurrentConsistencyLevels()) //
				.accessors(accessors).build();

	}

	public PropertyMeta<?, ?> parseWideMapProperty(PropertyParsingContext context)
	{
		validator.validateWideMapGenerics(context);

		Class<?> entityClass = context.getCurrentEntityClass();
		Field field = context.getCurrentField();
		PropertyType type = PropertyType.WIDE_MAP;
		MultiKeyProperties multiKeyProperties = null;
		CounterProperties counterProperties = null;

		Pair<Class<?>, Class<?>> types = determineMapGenericTypes(field);
		Class<?> keyClass = types.left;
		Class<?> valueClass = types.right;

		// Multi or Single Key
		multiKeyProperties = parseWideMapKey(multiKeyProperties, keyClass);

		if (context.isCounterType())
		{
			counterProperties = buildCounterProperties(valueClass, context);
			type = WIDE_MAP_COUNTER;
		}

		Validator.validateSerializable(valueClass, "Wide map value of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(entityClass, field);

		PropertyMeta<?, ?> propertyMeta = factory(keyClass, valueClass) //
				.objectMapper(context.getCurrentObjectMapper()) //
				.type(type) //
				.propertyName(context.getCurrentPropertyName()) //
				.accessors(accessors) //
				.multiKeyProperties(multiKeyProperties) //
				.counterProperties(counterProperties) //
				.consistencyLevels(context.getCurrentConsistencyLevels()) //
				.build();

		if (context.isCounterType())
		{
			context.getCounterMetas().add(propertyMeta);
		}

		saveExternalWideMapForDeferredBinding(context, propertyMeta);
		fillWideMapCustomConsistencyLevels(context, propertyMeta);

		return propertyMeta;
	}

	public <ID, V> void fillExternalWideMap(EntityParsingContext context,
			PropertyMeta<Void, ID> idMeta, PropertyMeta<?, V> propertyMeta, String externalTableName)
	{
		propertyMeta.setType(EXTERNAL_WIDE_MAP);
		GenericCompositeDao<ID, ?> dao;
		Cluster cluster = context.getCluster();
		Keyspace keyspace = context.getKeyspace();
		AchillesConfigurableConsistencyLevelPolicy configurableCLPolicy = context
				.getConfigurableCLPolicy();

		Class<V> valueClass = propertyMeta.getValueClass();

		if (!propertyMeta.type().isJoinColumn())
		{
			if (isSupportedType(valueClass))
			{
				dao = new GenericCompositeDao<ID, V>(cluster, keyspace, //
						idMeta.getValueSerializer(), //
						propertyMeta.getValueSerializer(), //
						externalTableName, configurableCLPolicy);
			}
			else
			{
				dao = new GenericCompositeDao<ID, String>(cluster, keyspace, //
						idMeta.getValueSerializer(), //
						STRING_SRZ, externalTableName, configurableCLPolicy);
			}
			context.getColumnFamilyDaosMap().put(externalTableName, dao);
		}
		propertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(
				externalTableName, idMeta.getValueSerializer()));
	}

	private void inferPropertyNameAndExternalTableName(PropertyParsingContext context)
	{
		String propertyName, externalTableName = null;
		Field field = context.getCurrentField();
		if (context.isJoinColumn())
		{
			JoinColumn column = field.getAnnotation(JoinColumn.class);
			externalTableName = field.getAnnotation(JoinColumn.class).table();
			propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
		}
		else if (context.isPrimaryKey())
		{
			propertyName = field.getName();
		}
		else
		{
			Column column = field.getAnnotation(Column.class);
			externalTableName = field.getAnnotation(Column.class).table();
			propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
		}

		context.setCurrentPropertyName(propertyName);
		context.setCurrentExternalTableName(externalTableName);
	}

	private Pair<Class<?>, Class<?>> determineMapGenericTypes(Field field)
	{
		Type genericType = field.getGenericType();
		ParameterizedType pt = (ParameterizedType) genericType;
		Type[] actualTypeArguments = pt.getActualTypeArguments();

		return new Pair<Class<?>, Class<?>>((Class<?>) actualTypeArguments[0],
				(Class<?>) actualTypeArguments[1]);
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
			PropertyParsingValidator
					.validateAllowedTypes(
							keyClass,
							allowedTypes,
							"The class '"
									+ keyClass.getCanonicalName()
									+ "' is not allowed as WideMap key. Did you forget to implement MultiKey interface ?");
		}
		return multiKeyProperties;
	}

	private String saveExternalWideMapForDeferredBinding(PropertyParsingContext context,
			PropertyMeta<?, ?> propertyMeta)
	{
		String externalTableName = context.isColumnFamilyDirectMapping() ? context
				.getCurrentColumnFamilyName() : context.getCurrentExternalTableName();

		if (StringUtils.isNotBlank(externalTableName) || context.isColumnFamilyDirectMapping())
		{
			if (context.isJoinColumn())
			{
				context.getJoinExternalWideMaps().put(propertyMeta, externalTableName);
			}
			else
			{
				context.getExternalWideMaps().put(propertyMeta, externalTableName);
			}
		}

		return externalTableName;
	}

	private void fillWideMapCustomConsistencyLevels(PropertyParsingContext context,
			PropertyMeta<?, ?> propertyMeta)
	{
		boolean isCustomConsistencyLevel = propertyHelper.hasConsistencyAnnotation(context
				.getCurrentField());
		String externalTableName = context.getCurrentExternalTableName();

		if (isCustomConsistencyLevel)
		{
			Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> consistencyLevels = propertyHelper
					.findConsistencyLevels(context.getCurrentField());

			context.getConfigurableCLPolicy().setConsistencyLevelForRead(
					consistencyLevels.right.left, externalTableName);
			context.getConfigurableCLPolicy().setConsistencyLevelForWrite(
					consistencyLevels.right.right, externalTableName);

			propertyMeta.setConsistencyLevels(consistencyLevels.left);
		}
	}

	private <T> CounterProperties buildCounterProperties(Class<T> valueClass,
			PropertyParsingContext context)
	{
		CounterProperties counterProperties;
		PropertyParsingValidator
				.validateAllowedTypes(
						valueClass,
						allowedCounterTypes,
						"Wrong counter type for the field '"
								+ context.getCurrentField().getName()
								+ "'. Only java.lang.Long and primitive long are allowed for @Counter types");
		counterProperties = new CounterProperties(context.getCurrentEntityClass()
				.getCanonicalName());
		return counterProperties;
	}

	private void parseSimpleCounterConsistencyLevel(PropertyParsingContext context,
			PropertyMeta<?, ?> propertyMeta)
	{

		Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = propertyHelper
				.findConsistencyLevels(context.getCurrentField()).left;

		validator.validateConsistencyLevelForCounter(context, consistencyLevels);

		propertyMeta.setConsistencyLevels(consistencyLevels);
	}

}
