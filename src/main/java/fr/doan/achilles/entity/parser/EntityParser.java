package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.exception.BeanMappingException;
import fr.doan.achilles.validation.Validator;

/**
 * EntityParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParser
{
	private PropertyParser parser = new PropertyParser();
	private PropertyFilter filter = new PropertyFilter();
	private EntityHelper helper = new EntityHelper();

	public EntityMeta<?> parseEntity(Keyspace keyspace, Class<?> entityClass,
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled)
	{
		Map<Field, String> externalWideMaps = new HashMap<Field, String>();
		Map<Field, String> externalJoinWideMaps = new HashMap<Field, String>();

		Validator.validateInstantiable(entityClass);
		String columnFamily = helper.inferColumnFamilyName(entityClass, entityClass.getName());
		Long serialVersionUID = helper.findSerialVersionUID(entityClass);
		boolean wideRow = entityClass.getAnnotation(fr.doan.achilles.annotations.WideRow.class) != null ? true
				: false;

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Void, ?> idMeta = null;

		List<Field> inheritedFields = helper.getInheritedPrivateFields(entityClass);

		for (Field field : inheritedFields)
		{
			boolean isWideMap = WideMap.class.isAssignableFrom(field.getType());
			if (filter.hasAnnotation(field, Id.class))
			{
				idMeta = parser.parse(entityClass, field, field.getName());
				continue;
			}

			else if (filter.hasAnnotation(field, Column.class))
			{
				Column column = field.getAnnotation(Column.class);
				String externalTableName = field.getAnnotation(Column.class).table();
				String propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field
						.getName();

				Validator.validateFalse(propertyMetas.containsKey(propertyName),
						"The property '" + propertyName + "' is already used for the entity '"
								+ entityClass.getCanonicalName() + "'");

				if (StringUtils.isNotBlank(externalTableName) && isWideMap)
				{
					externalWideMaps.put(field, propertyName);
				}
				else
				{
					propertyMetas.put(propertyName, parser.parse(entityClass, field, propertyName));
				}
			}
			else if (filter.hasAnnotation(field, JoinColumn.class))
			{
				JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
				String externalTableName = field.getAnnotation(JoinColumn.class).table();

				String propertyName = StringUtils.isNotBlank(joinColumn.name()) ? joinColumn.name()
						: field.getName();
				Validator.validateFalse(propertyMetas.containsKey(propertyName),
						"The property '" + propertyName + "' is already used for the entity '"
								+ entityClass.getCanonicalName() + "'");

				if (StringUtils.isNotBlank(externalTableName) && isWideMap)
				{
					externalJoinWideMaps.put(field, propertyName);
				}
				else
				{
					PropertyMeta<?, ?> joinPropertyMeta = parser.parse(entityClass, field,
							propertyName);
					propertyMetas.put(propertyName, joinPropertyMeta);
					joinPropertyMetaToBeFilled.put(joinPropertyMeta,
							joinPropertyMeta.getValueClass());
				}
			}

		}

		validateIdMeta(entityClass, idMeta);
		validatePropertyMetas(entityClass, propertyMetas);
		validateWideRow(entityClass, wideRow, propertyMetas);

		// Deferred external wide map fields parsing
		for (Entry<Field, String> entry : externalWideMaps.entrySet())
		{
			String propertyName = entry.getValue();
			propertyMetas.put(
					propertyName,
					parser.parseExternalWideMapProperty(keyspace, idMeta, entityClass,
							entry.getKey(), propertyName));
		}

		// Deferred external join wide map fields parsing
		for (Entry<Field, String> entry : externalJoinWideMaps.entrySet())
		{
			String propertyName = entry.getValue();
			PropertyMeta<Object, Object> externalJoinWideMapMeta = parser
					.parseExternalJoinWideMapProperty(keyspace, idMeta, entityClass,
							entry.getKey(), propertyName);
			propertyMetas.put(propertyName, externalJoinWideMapMeta);

			joinPropertyMetaToBeFilled.put(externalJoinWideMapMeta,
					externalJoinWideMapMeta.getValueClass());
		}

		return entityMetaBuilder(idMeta).keyspace(keyspace)
				.className(entityClass.getCanonicalName()) //
				.columnFamilyName(columnFamily) //
				.serialVersionUID(serialVersionUID) //
				.propertyMetas(propertyMetas) //
				.wideRow(wideRow) //
				.build();
	}

	private void validateIdMeta(Class<?> entityClass, PropertyMeta<Void, ?> idMeta)
	{
		if (idMeta == null)
		{
			throw new BeanMappingException("The entity '" + entityClass.getCanonicalName()
					+ "' should have at least one field with javax.persistence.Id annotation");
		}
	}

	private void validatePropertyMetas(Class<?> entityClass,
			Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		if (propertyMetas.isEmpty())
		{
			throw new BeanMappingException(
					"The entity '"
							+ entityClass.getCanonicalName()
							+ "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");
		}
	}

	private void validateWideRow(Class<?> entityClass, boolean wideRow,
			Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		if (wideRow)
		{
			if (propertyMetas != null && propertyMetas.size() > 1)
			{
				throw new BeanMappingException("The WideRow entity '"
						+ entityClass.getCanonicalName()
						+ "' should not have more than one property annotated with @Column");
			}

			PropertyType type = propertyMetas.entrySet().iterator().next().getValue().type();

			if (type != PropertyType.WIDE_MAP)
			{
				throw new BeanMappingException("The WideRow entity '"
						+ entityClass.getCanonicalName()
						+ "' should have one and only one @Column of type WideMap");
			}
		}
	}

}
