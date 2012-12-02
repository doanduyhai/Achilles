package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Table;

import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.validation.Validator;

public class EntityParser
{

	private PropertyParser parser = new PropertyParser();

	public EntityMeta<?> parseEntity(Keyspace keyspace, Class<?> entityClass)
	{
		Validator.validateInstantiable(entityClass);
		String canonicalName = findCanonicalName(entityClass);
		String columnFamily = inferColumnFamilyName(entityClass, canonicalName);
		Long serialVersionUID = findSerialVersionUID(entityClass);

		Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
		PropertyMeta<?> idMeta = null;

		for (Field field : entityClass.getDeclaredFields())
		{
			if (field.getAnnotation(javax.persistence.Id.class) != null)
			{
				idMeta = parser.parse(entityClass, field, field.getName());
			}

			else if (field.getAnnotation(javax.persistence.Column.class) != null)
			{
				Column column = field.getAnnotation(javax.persistence.Column.class);
				String propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
				propertyMetas.put(propertyName, parser.parse(entityClass, field, propertyName));
			}

		}

		if (idMeta == null)
		{
			throw new IncorrectTypeException("The entity '" + entityClass.getCanonicalName()
					+ "' should have at least one field with javax.persistence.Id annotation");
		}

		if (propertyMetas.isEmpty())
		{
			throw new IncorrectTypeException("The entity '" + entityClass.getCanonicalName()
					+ "' should have at least one field with javax.persistence.Column annotation");
		}

		return entityMetaBuilder(idMeta).keyspace(keyspace).canonicalClassName(canonicalName).columnFamilyName(columnFamily)
				.serialVersionUID(serialVersionUID).propertyMetas(propertyMetas).build();
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
				throw new IncorrectTypeException("The 'serialVersionUID' property should be publicly accessible for entity '"
						+ entity.getCanonicalName() + "'");
			}
		}
		catch (NoSuchFieldException e)
		{
			throw new IncorrectTypeException("The 'serialVersionUID' property should be declared for entity '" + entity.getCanonicalName() + "'", e);
		}
		catch (IllegalAccessException e)
		{
			throw new IncorrectTypeException("The 'serialVersionUID' property should be publicly accessible for entity '" + entity.getCanonicalName()
					+ "'", e);
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
			throw new IncorrectTypeException("The entity '" + entity.getCanonicalName() + "' should have javax.persistence.Table annotation");
		}
		return columnFamily;
	}

}
