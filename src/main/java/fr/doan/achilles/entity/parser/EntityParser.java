package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Table;

import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.validation.Validator;

public class EntityParser
{
	private PropertyParser parser = new PropertyParser();

	private PropertyFilter filter = new PropertyFilter();

	public EntityMeta<?> parseEntity(Keyspace keyspace, Class<?> entityClass,
			Map<Class<?>, EntityMeta<?>> entityMetaMap, ColumnFamilyHelper columnFamilyHelper,
			boolean forceColumnFamilyCreation)
	{
		Validator.validateInstantiable(entityClass);
		String canonicalName = findCanonicalName(entityClass);
		String columnFamily = inferColumnFamilyName(entityClass, canonicalName);
		Long serialVersionUID = findSerialVersionUID(entityClass);

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Void, ?> idMeta = null;

		List<Field> inheritedFields = getInheritedPrivateFields(entityClass);

		for (Field field : inheritedFields)
		{
			if (filter.hasAnnotation(field, Id.class))
			{
				idMeta = parser.parse(entityClass, field, field.getName());
			}

			else if (filter.hasAnnotation(field, Column.class))
			{
				Column column = field.getAnnotation(Column.class);
				String propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field
						.getName();
				propertyMetas.put(propertyName, parser.parse(entityClass, field, propertyName));
			}
			else if (filter.hasAnnotation(field, JoinColumn.class))
			{

			}

		}

		if (idMeta == null)
		{
			throw new IncorrectTypeException("The entity '" + entityClass.getCanonicalName()
					+ "' should have at least one field with javax.persistence.Id annotation");
		}

		if (propertyMetas.isEmpty())
		{
			throw new IncorrectTypeException(
					"The entity '"
							+ entityClass.getCanonicalName()
							+ "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");
		}

		return entityMetaBuilder(idMeta).keyspace(keyspace).canonicalClassName(canonicalName)
				.columnFamilyName(columnFamily).serialVersionUID(serialVersionUID)
				.propertyMetas(propertyMetas).build();
	}

	private String findCanonicalName(Class<?> entity)
	{
		if (StringUtils.isNotBlank(entity.getCanonicalName()))
		{
			return entity.getCanonicalName();
		}
		else
		{
			return entity.getName();
		}
	}

	private Long findSerialVersionUID(Class<?> entity)
	{
		Long serialVersionUID = null;
		try
		{
			Field declaredSerialVersionUID = entity.getDeclaredField("serialVersionUID");
			declaredSerialVersionUID.setAccessible(true);
			serialVersionUID = declaredSerialVersionUID.getLong(null);

			if (!Modifier.isPublic(declaredSerialVersionUID.getModifiers()))
			{
				throw new IncorrectTypeException(
						"The 'serialVersionUID' property should be publicly accessible for entity '"
								+ entity.getCanonicalName() + "'");
			}
		}
		catch (NoSuchFieldException e)
		{
			throw new IncorrectTypeException(
					"The 'serialVersionUID' property should be declared for entity '"
							+ entity.getCanonicalName() + "'", e);
		}
		catch (IllegalAccessException e)
		{
			throw new IncorrectTypeException(
					"The 'serialVersionUID' property should be publicly accessible for entity '"
							+ entity.getCanonicalName() + "'", e);
		}
		return serialVersionUID;
	}

	private String inferColumnFamilyName(Class<?> entity, String canonicalName)
	{
		Table table = entity.getAnnotation(javax.persistence.Table.class);
		String columnFamily;
		if (table != null)
		{
			if (StringUtils.isNotBlank(table.name()))
			{
				columnFamily = table.name();

			}
			else
			{
				columnFamily = canonicalName;
			}
		}
		else
		{
			throw new IncorrectTypeException("The entity '" + entity.getCanonicalName()
					+ "' should have javax.persistence.Table annotation");
		}
		return columnFamily;
	}

	public List<Field> getInheritedPrivateFields(Class<?> type)
	{
		List<Field> result = new ArrayList<Field>();

		Class<?> i = type;
		while (i != null && i != Object.class)
		{
			for (Field declaredField : i.getDeclaredFields())
			{
				if (filter.matches(declaredField))
				{
					result.add(declaredField);
				}
			}
			i = i.getSuperclass();
		}

		return result;
	}

	public Field getInheritedPrivateFields(Class<?> type, Class<?> annotation)
	{
		Class<?> i = type;
		while (i != null && i != Object.class)
		{
			for (Field declaredField : i.getDeclaredFields())
			{
				if (filter.matches(declaredField, annotation))
				{
					return declaredField;
				}
			}
			i = i.getSuperclass();
		}
		return null;
	}

	public Field getInheritedPrivateFields(Class<?> type, Class<?> annotation, String name)
	{
		Class<?> i = type;
		while (i != null && i != Object.class)
		{
			for (Field declaredField : i.getDeclaredFields())
			{
				if (filter.matches(declaredField, annotation, name))
				{
					return declaredField;
				}
			}
			i = i.getSuperclass();
		}
		return null;
	}
}
