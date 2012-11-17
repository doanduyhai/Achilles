package fr.doan.achilles.parser;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Table;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.exception.NotSerializableException;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;

public class EntityParser
{

	public <ID extends Serializable> EntityMeta<ID> parseEntity(Class<?> entity)
	{
		Class<ID> idClass = null;
		String canonicalName = determineCanonicalName(entity);
		String columnFamily = determineColumnFamily(entity, canonicalName);
		Long serialVersionUID = determineSerialVersionUID(entity);
		Map<String, PropertyMeta<?>> propertieMetas = new HashMap<String, PropertyMeta<?>>();
		for (Field field : entity.getDeclaredFields())
		{
			if (field.getAnnotation(javax.persistence.Id.class) != null)
			{
				idClass = this.determineIdClass(field);
			}

			else if (field.getAnnotation(javax.persistence.Column.class) != null)
			{
				Column column = field.getAnnotation(javax.persistence.Column.class);
				String propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
				propertieMetas.put(propertyName, PropertyParser.parse(field, propertyName));
			}
		}

		if (idClass == null)
		{
			throw new IncorrectTypeException("The entity '" + entity.getCanonicalName()
					+ "' should have at least one field with javax.persistence.Id annotation");
		}

		if (propertieMetas.isEmpty())
		{
			throw new IncorrectTypeException("The entity '" + entity.getCanonicalName()
					+ "' should have at least one field with javax.persistence.Column annotation");
		}

		return new EntityMeta<ID>(idClass, canonicalName, columnFamily, serialVersionUID, propertieMetas);
	}

	private String determineCanonicalName(Class<?> entity)
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

	@SuppressWarnings("unchecked")
	private <ID extends Serializable> Class<ID> determineIdClass(Field field)
	{
		Class<?> idClass = field.getType();
		if (Serializable.class.isAssignableFrom(idClass))
		{
			return (Class<ID>) idClass;
		}
		else
		{
			throw new NotSerializableException("The @Id field '" + field.getName() + "' should implements Serializable");
		}

	}

	private Long determineSerialVersionUID(Class<?> entity)
	{
		Long serialVersionUID = null;
		try
		{
			Field declaredSerialVersionUID = entity.getDeclaredField("serialVersionUID");
			declaredSerialVersionUID.setAccessible(true);
			serialVersionUID = declaredSerialVersionUID.getLong(null);
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

	private String determineColumnFamily(Class<?> entity, String canonicalName)
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
