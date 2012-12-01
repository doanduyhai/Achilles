package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.builder.ListPropertyMetaBuilder.listPropertyMetaBuilder;
import static fr.doan.achilles.entity.metadata.builder.MapPropertyMetaBuilder.mapPropertyMetaBuilder;
import static fr.doan.achilles.entity.metadata.builder.SetPropertyMetaBuilder.setPropertyMetaBuilder;
import static fr.doan.achilles.entity.metadata.builder.SimplePropertyMetaBuilder.simplePropertyMetaBuilder;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import fr.doan.achilles.annotations.Lazy;
import fr.doan.achilles.entity.EntityPropertyHelper;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.validation.Validator;

public class PropertyParser
{

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

		else
		{
			propertyMeta = parseSimpleProperty(beanClass, field, propertyName);
		}
		return propertyMeta;
	}

	@SuppressWarnings("unchecked")
	private <V> PropertyMeta<V> parseSimpleProperty(Class<?> beanClass, Field field, String propertyName)
	{
		Validator.validateSerializable(field.getType(), field.getName());
		Method[] accessors = helper.findAccessors(beanClass, field);
		boolean lazy = this.isLazy(field);
		return simplePropertyMetaBuilder((Class<V>) field.getType()).propertyName(propertyName).accessors(accessors).lazy(lazy).build();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private <V> PropertyMeta<V> parseListProperty(Class<?> beanClass, Field field, String propertyName, Class<?> fieldType)
	{

		Class valueClass;
		Type genericType = field.getGenericType();

		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 0)
			{
				valueClass = (Class) actualTypeArguments[0];
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

		Validator.validateSerializable(valueClass, "value type of " + field.getName());
		Method[] accessors = helper.findAccessors(beanClass, field);
		boolean lazy = this.isLazy(field);
		return listPropertyMetaBuilder((Class<V>) valueClass).propertyName(propertyName).accessors(accessors).lazy(lazy).build();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private <V> PropertyMeta<V> parseSetProperty(Class<?> beanClass, Field field, String propertyName, Class<?> fieldType)
	{

		Class valueClass;
		Type genericType = field.getGenericType();

		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 0)
			{
				valueClass = (Class) actualTypeArguments[0];
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
		Validator.validateSerializable(valueClass, "value type of " + field.getName());
		Method[] accessors = helper.findAccessors(beanClass, field);
		boolean lazy = this.isLazy(field);
		return setPropertyMetaBuilder((Class<V>) valueClass).propertyName(propertyName).accessors(accessors).lazy(lazy).build();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private <V> PropertyMeta<V> parseMapProperty(Class<?> beanClass, Field field, String propertyName, Class<?> fieldType)
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
		Validator.validateSerializable(valueClass, "value type of " + field.getName());
		Validator.validateSerializable(keyType, "key type of " + field.getName());
		Method[] accessors = helper.findAccessors(beanClass, field);
		boolean lazy = this.isLazy(field);
		return mapPropertyMetaBuilder(keyType, valueClass).propertyName(propertyName).accessors(accessors).lazy(lazy).build();

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
