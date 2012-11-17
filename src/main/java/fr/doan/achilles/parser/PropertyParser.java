package fr.doan.achilles.parser;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.SetPropertyMeta;
import fr.doan.achilles.metadata.SimplePropertyMeta;

public class PropertyParser
{
	@SuppressWarnings("unchecked")
	public static <V extends Serializable> PropertyMeta<V> parse(Field field, String propertyName)
	{
		Class<?> fieldType = field.getType();

		PropertyMeta<V> propertyMeta;

		if (List.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseListProperty(field, propertyName, fieldType);
		}

		else if (Set.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseSetProperty(field, propertyName, fieldType);
		}

		else if (Map.class.isAssignableFrom(fieldType))
		{
			propertyMeta = parseMapProperty(field, propertyName, fieldType);
		}
		else
		{
			propertyMeta = new SimplePropertyMeta<V>(propertyName, (Class<V>) fieldType);
		}

		return propertyMeta;
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private static <V extends Serializable> PropertyMeta<V> parseMapProperty(Field field, String propertyName, Class<?> fieldType)
	{
		PropertyMeta<V> propertyMeta;
		Class<V> valueClass;
		Class<? extends Serializable> keyClass;
		Class<? extends Map> mapType = (Class<? extends Map>) fieldType;
		Type genericType = field.getGenericType();

		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 1)
			{
				keyClass = (Class<? extends Serializable>) actualTypeArguments[0];
				valueClass = (Class<V>) actualTypeArguments[1];
			}
			else
			{
				keyClass = (Class<? extends Serializable>) Object.class;
				valueClass = (Class<V>) Object.class;
			}
		}
		else
		{
			keyClass = (Class<? extends Serializable>) Object.class;
			valueClass = (Class<V>) Object.class;
		}

		propertyMeta = new MapPropertyMeta<V>(propertyName, keyClass, valueClass, mapType);
		return propertyMeta;
	}

	@SuppressWarnings("unchecked")
	private static <V extends Serializable> PropertyMeta<V> parseSetProperty(Field field, String propertyName, Class<?> fieldType)
	{
		PropertyMeta<V> propertyMeta;
		Class<V> valueClass;
		Class<? extends Set<V>> setType = (Class<? extends Set<V>>) fieldType;
		Type genericType = field.getGenericType();

		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 0)
			{
				valueClass = (Class<V>) actualTypeArguments[0];
			}
			else
			{
				valueClass = (Class<V>) Object.class;
			}
		}
		else
		{
			valueClass = (Class<V>) Object.class;
		}

		propertyMeta = new SetPropertyMeta<V>(propertyName, valueClass, setType);
		return propertyMeta;
	}

	@SuppressWarnings("unchecked")
	private static <V extends Serializable> PropertyMeta<V> parseListProperty(Field field, String propertyName, Class<?> fieldType)
	{
		PropertyMeta<V> propertyMeta;
		Class<V> valueClass;
		Class<? extends List<V>> listType = (Class<? extends List<V>>) fieldType;
		Type genericType = field.getGenericType();

		if (genericType instanceof ParameterizedType)
		{
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length > 0)
			{
				valueClass = (Class<V>) actualTypeArguments[0];
			}
			else
			{
				valueClass = (Class<V>) Object.class;
			}
		}
		else
		{
			valueClass = (Class<V>) Object.class;
		}

		propertyMeta = new ListPropertyMeta<V>(propertyName, valueClass, listType);
		return propertyMeta;
	}
}
