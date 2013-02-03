package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_JOIN_WIDE_MAP;
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

import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import me.prettyprint.hector.api.Keyspace;

/**
 * PropertyParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyParser
{

	private PropertyFilter filter = new PropertyFilter();

	public PropertyParser() {}

	private PropertyHelper propertyHelper = new PropertyHelper();
	private EntityHelper entityHelper = new EntityHelper();

	@SuppressWarnings("unchecked")
	public <K, V> PropertyMeta<K, V> parse(Class<?> beanClass, Field field, String propertyName)
	{
		Class<?> fieldType = field.getType();

		PropertyMeta<K, V> propertyMeta;

		if (List.class.isAssignableFrom(fieldType))
		{
			propertyMeta = (PropertyMeta<K, V>) parseListProperty(beanClass, field, propertyName);
		}

		else if (Set.class.isAssignableFrom(fieldType))
		{
			propertyMeta = (PropertyMeta<K, V>) parseSetProperty(beanClass, field, propertyName);
		}

		else if (Map.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseMapProperty(beanClass, field, propertyName);
		}

		else if (WideMap.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseWideMapProperty(beanClass, field, propertyName);
		}

		else
		{
			propertyMeta = (PropertyMeta<K, V>) parseSimpleProperty(beanClass, field, propertyName);
		}
		return propertyMeta;
	}

	@SuppressWarnings("unchecked")
	private <V> PropertyMeta<Void, V> parseSimpleProperty(Class<?> beanClass, Field field,
			String propertyName)
	{
		Validator.validateSerializable(field.getType(), "Value of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);

		PropertyMeta<Void, V> propertyMeta = null;
		if (filter.hasAnnotation(field, JoinColumn.class))
		{
			PropertyType type = PropertyType.JOIN_SIMPLE;

			JoinProperties joinProperties = new JoinProperties();

			if (filter.hasAnnotation(field, OneToMany.class)
					|| filter.hasAnnotation(field, ManyToMany.class))
			{
				throw new BeanMappingException(
						"Incorrect annotation. Only @OneToOne or @ManyToOne is allowed for the join property '"
								+ field.getName() + "'");
			}

			else if (filter.hasAnnotation(field, ManyToOne.class))
			{
				ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);
				joinProperties.addCascadeType(Arrays.asList(manyToOne.cascade()));
			}
			else if (filter.hasAnnotation(field, OneToOne.class))
			{
				OneToOne oneToOne = field.getAnnotation(OneToOne.class);
				joinProperties.addCascadeType(Arrays.asList(oneToOne.cascade()));
			}
			else
			{
				throw new BeanMappingException(
						"Missing @ManyToOne annotation for the join property '" + field.getName()
								+ "'");
			}

			propertyMeta = factory((Class<V>) field.getType()) //
					.type(type) //
					.propertyName(propertyName).accessors(accessors) //
					.joinProperties(joinProperties).build();
		}
		else
		{

			PropertyType type = propertyHelper.isLazy(field) ? LAZY_SIMPLE : SIMPLE;
			propertyMeta = factory((Class<V>) field.getType()).type(type)
					.propertyName(propertyName).accessors(accessors).build();
		}

		return propertyMeta;
	}

	@SuppressWarnings("unchecked")
	private <V> PropertyMeta<Void, V> parseListProperty(Class<?> beanClass, Field field,
			String propertyName)
	{

		Class<?> valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);

		Validator.validateSerializable(valueClass, "List value type of '" + field.getName()
				+ "' should be Serializable");
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
			String propertyName)
	{

		Class valueClass;
		Type genericType = field.getGenericType();

		valueClass = propertyHelper.inferValueClass(genericType);
		Validator.validateSerializable(valueClass, "Set value type of '" + field.getName()
				+ "' should be Serializable");
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
			String propertyName)
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
		Validator.validateSerializable(valueClass, "Map value type of '" + field.getName()
				+ "' should be Serializable");
		Validator.validateSerializable(keyType, "Map key type of '" + field.getName()
				+ "' should be Serializable");
		Method[] accessors = entityHelper.findAccessors(beanClass, field);
		PropertyType type = propertyHelper.isLazy(field) ? LAZY_MAP : MAP;

		return factory(keyType, valueClass).type(type).propertyName(propertyName)
				.accessors(accessors).build();

	}

	public <K, V, ID> PropertyMeta<K, V> parseExternalWideMapProperty(Keyspace keyspace,
			PropertyMeta<Void, ID> idMeta, Class<?> beanClass, Field field, String propertyName)
	{
		String externalColumnFamilyName = field.getAnnotation(Column.class).table();
		PropertyMeta<K, V> propertyMeta = this.parseWideMapProperty(beanClass, field, propertyName);
		propertyMeta.setType(EXTERNAL_WIDE_MAP);
		GenericCompositeDao<ID, V> dao = new GenericCompositeDao<ID, V>(keyspace,
				idMeta.getValueSerializer(), propertyMeta.getValueSerializer(),
				externalColumnFamilyName);

		propertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(
				externalColumnFamilyName, dao, idMeta.getValueSerializer()));

		return propertyMeta;
	}

	public <ID, JOIN_ID, K, V> PropertyMeta<K, V> parseExternalJoinWideMapProperty(
			Keyspace keyspace, PropertyMeta<Void, ID> idMeta, Class<?> beanClass, Field field,
			String propertyName)
	{
		PropertyMeta<K, V> propertyMeta = this.parseWideMapProperty(beanClass, field, propertyName);

		propertyMeta.setType(EXTERNAL_JOIN_WIDE_MAP);
		String externalColumnFamilyName = field.getAnnotation(JoinColumn.class).table();
		propertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(
				externalColumnFamilyName, null, idMeta.getValueSerializer()));

		return propertyMeta;
	}

	@SuppressWarnings("unchecked")
	private <K, V> PropertyMeta<K, V> parseWideMapProperty(Class<?> beanClass, Field field,
			String propertyName)
	{

		PropertyType type = PropertyType.WIDE_MAP;
		MultiKeyProperties multiKeyProperties = null;
		JoinProperties joinProperties = null;
		Class<K> keyClass = null;
		Class<V> valueClass = null;

		// Multi or Single Key
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
					multiKeyProperties = propertyHelper.parseMultiKey(keyClass);
				}
				else
				{
					Validator
							.validateAllowedTypes(
									keyClass,
									propertyHelper.allowedTypes,
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

		if (filter.hasAnnotation(field, JoinColumn.class))
		{
			type = PropertyType.JOIN_WIDE_MAP;

			joinProperties = new JoinProperties();

			if (filter.hasAnnotation(field, OneToOne.class)
					|| filter.hasAnnotation(field, ManyToOne.class))
			{
				throw new BeanMappingException(
						"Incorrect annotation. Only @OneToMany or @ManyToMany is allowed for the join property '"
								+ field.getName() + "'");
			}

			else if (filter.hasAnnotation(field, ManyToMany.class))
			{
				ManyToMany manyToMany = field.getAnnotation(ManyToMany.class);
				joinProperties.addCascadeType(Arrays.asList(manyToMany.cascade()));
			}
			else if (filter.hasAnnotation(field, OneToMany.class))
			{
				OneToMany oneToMany = field.getAnnotation(OneToMany.class);
				joinProperties.addCascadeType(Arrays.asList(oneToMany.cascade()));
			}

			else
			{
				throw new BeanMappingException(
						"Missing @ManyToMany annotation for the join property '" + field.getName()
								+ "'");
			}

		}

		return factory(keyClass, valueClass) //
				.type(type) //
				.propertyName(propertyName) //
				.accessors(accessors) //
				.multiKeyProperties(multiKeyProperties) //
				.joinProperties(joinProperties) //
				.build();
	}
}
