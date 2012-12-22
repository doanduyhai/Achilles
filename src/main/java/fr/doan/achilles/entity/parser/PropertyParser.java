package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SET;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.factory.PropertyMetaFactory.factory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Table;

import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.PropertyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.type.MultiKey;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.validation.Validator;

public class PropertyParser
{

	private Set<Class<?>> wideMapType = new HashSet<Class<?>>();

	public PropertyParser() {
		// Wide Map
		wideMapType.add(WideMap.class);
	}

	private PropertyHelper propertyHelper = new PropertyHelper();
	private EntityHelper entityHelper = new EntityHelper();

	@SuppressWarnings("unchecked")
	public <K, V> PropertyMeta<K, V> parse(Class<?> beanClass, Field field, String propertyName)
	{
		Class<?> fieldType = field.getType();

		PropertyMeta<K, V> propertyMeta;

		if (List.class.isAssignableFrom(fieldType))
		{
			propertyMeta = (PropertyMeta<K, V>) parseListProperty(beanClass, field, propertyName,
					fieldType);
		}

		else if (Set.class.isAssignableFrom(fieldType))
		{
			propertyMeta = (PropertyMeta<K, V>) parseSetProperty(beanClass, field, propertyName,
					fieldType);
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
			propertyMeta = (PropertyMeta<K, V>) parseSimpleProperty(beanClass, field, propertyName);
		}
		return propertyMeta;
	}

	@SuppressWarnings("unchecked")
	public <K, V> PropertyMeta<K, V> parseJoinColum(Class<?> beanClass, Field field,
			Map<Class<?>, EntityMeta<?>> entityMetaMap, Keyspace keyspace,
			EntityParser entityParser, ColumnFamilyHelper columnFamilyHelper,
			boolean forceColumnFamilyCreation)
	{
		Validator.validateAllowedTypes(field.getType(), wideMapType,
				"The JoinColumn '" + field.getName() + "' should be of type WideMap");

		JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);

		List<Class<?>> componentClasses = new ArrayList<Class<?>>();
		List<Method> componentGetters = new ArrayList<Method>();
		List<Method> componentSetters = new ArrayList<Method>();

		Class<K> keyClass = (Class<K>) propertyHelper.inferKeyClass(field.getGenericType());
		if (MultiKey.class.isAssignableFrom(keyClass))
		{
			propertyHelper.parseMultiKey(componentClasses, componentGetters, componentSetters,
					keyClass);
		}
		else
		{
			Validator.validateAllowedTypes(keyClass, propertyHelper.allowedTypes, "The class '"
					+ keyClass.getCanonicalName() + "' is not allowed as WideMap key");
		}

		String propertyName = StringUtils.isNotBlank(joinColumn.name()) ? joinColumn.name() : field
				.getName();
		boolean insertable = joinColumn.insertable();
		boolean entityValue = false;

		Method idGetter;
		String externalColumnFamily;
		Class<V> valueClass = (Class<V>) propertyHelper.inferValueClass(field.getGenericType());
		Class<?> idClass;

		if (valueClass.getAnnotation(Table.class) != null)
		{
			EntityMeta<?> valueEntityMeta;
			if (!entityMetaMap.containsKey(valueClass))
			{
				valueEntityMeta = entityParser.parseEntity(keyspace, valueClass, entityMetaMap,
						columnFamilyHelper, forceColumnFamilyCreation);
				entityMetaMap.put(valueClass, valueEntityMeta);
			}
			else
			{
				valueEntityMeta = entityMetaMap.get(valueClass);
			}

			entityValue = true;
			idGetter = valueEntityMeta.getIdMeta().getGetter();
			idClass = valueEntityMeta.getIdMeta().getValueClass();
			externalColumnFamily = valueEntityMeta.getColumnFamilyName();
		}
		else
		{
			externalColumnFamily = joinColumn.table();
			Validator.validateNotBlank(externalColumnFamily,
					"The 'table' parameter should be set for the @JoinColumn annotation on field '"
							+ field.getName() + "'");

			Field id = entityHelper.getInheritedPrivateFields(beanClass, Id.class);
			idClass = id.getType();
			idGetter = entityHelper.findGetter(beanClass, id);
			columnFamilyHelper.validateWideRow(externalColumnFamily, forceColumnFamilyCreation,
					idClass, keyClass, valueClass);
		}
		Validator.validateSerializable(valueClass, "value type of '" + field.getName() + "'");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);

		if (componentClasses.size() == 0)
		{
			return factory(keyClass, valueClass) //
					.type(JOIN_WIDE_MAP) //
					.propertyName(propertyName) //
					.accessors(accessors) //
					.singleKey(true) //
					.insertable(insertable) //
					.entityValue(entityValue) //
					.joinColumnFamily(externalColumnFamily) //
					.idGetter(idGetter) //
					.idClass(idClass) //
					.build();
		}
		else
		{
			return factory(keyClass, valueClass) //
					.type(JOIN_WIDE_MAP) //
					.propertyName(propertyName) //
					.accessors(accessors) //
					.singleKey(false) //
					.componentClasses(componentClasses) //
					.componentGetters(componentGetters) //
					.componentSetters(componentSetters) //
					.insertable(insertable) //
					.entityValue(entityValue) //
					.joinColumnFamily(externalColumnFamily) //
					.idGetter(idGetter) //
					.idClass(idClass) //
					.build();
		}
	}

	@SuppressWarnings("unchecked")
	private <V> PropertyMeta<Void, V> parseSimpleProperty(Class<?> beanClass, Field field,
			String propertyName)
	{
		Validator.validateSerializable(field.getType(), "property '" + field.getName() + "'");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_SIMPLE : SIMPLE;

		return factory((Class<V>) field.getType()).type(type).propertyName(propertyName)
				.accessors(accessors).build();

	}

	@SuppressWarnings("unchecked")
	private <V> PropertyMeta<Void, V> parseListProperty(Class<?> beanClass, Field field,
			String propertyName, Class<?> fieldType)
	{

		Class<?> valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);

		Validator.validateSerializable(valueClass, "list value type of '" + field.getName() + "'");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_LIST : LIST;

		return factory((Class<V>) valueClass).type(type).propertyName(propertyName)
				.accessors(accessors).build();

	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private <V> PropertyMeta<Void, V> parseSetProperty(Class<?> beanClass, Field field,
			String propertyName, Class<?> fieldType)
	{

		Class valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);
		Validator.validateSerializable(valueClass, "set value type of '" + field.getName() + "'");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_SET : SET;

		return factory((Class<V>) valueClass).type(type).propertyName(propertyName)
				.accessors(accessors).build();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private <K, V> PropertyMeta<K, V> parseMapProperty(Class<?> beanClass, Field field,
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
		Method[] accessors = entityHelper.findAccessors(beanClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_MAP : MAP;

		return factory(keyType, valueClass).type(type).propertyName(propertyName)
				.accessors(accessors).build();

	}

	@SuppressWarnings("unchecked")
	private <K, V> PropertyMeta<K, V> parseWideMapProperty(Class<?> beanClass, Field field,
			String propertyName, Class<?> fieldType)
	{
		List<Class<?>> componentClasses = new ArrayList<Class<?>>();
		List<Method> componentGetters = new ArrayList<Method>();
		List<Method> componentSetters = new ArrayList<Method>();

		Class<K> keyClass;
		Class<V> valueClass;

		Type genericType = field.getGenericType();
		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 1)
			{
				keyClass = (Class<K>) actualTypeArguments[0];
				valueClass = (Class<V>) actualTypeArguments[1];

				if (MultiKey.class.isAssignableFrom(keyClass))
				{
					propertyHelper.parseMultiKey(componentClasses, componentGetters,
							componentSetters, keyClass);
				}
				else
				{
					Validator.validateAllowedTypes(keyClass, propertyHelper.allowedTypes,
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
		Method[] accessors = entityHelper.findAccessors(beanClass, field);
		if (componentClasses.size() == 0)
		{
			return factory(keyClass, valueClass).type(PropertyType.WIDE_MAP)
					.propertyName(propertyName).accessors(accessors).singleKey(true).build();

		}
		else
		{
			return factory(keyClass, valueClass).type(PropertyType.WIDE_MAP)
					.propertyName(propertyName).accessors(accessors).singleKey(false) //
					.componentClasses(componentClasses) //
					.componentGetters(componentGetters) //
					.componentSetters(componentSetters) //
					.build();
		}
	}
}
