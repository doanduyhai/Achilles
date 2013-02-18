package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.validation.Validator;

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
import org.codehaus.jackson.map.ObjectMapper;

/**
 * EntityParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParser
{
	PropertyParser parser = new PropertyParser();
	PropertyFilter filter = new PropertyFilter();
	EntityHelper helper = new EntityHelper();
	private ObjectMapperFactory objectMapperFactory;

	public EntityParser(ObjectMapperFactory objectMapperFactory) {
		Validator.validateNotNull(objectMapperFactory,
				"A non null ObjectMapperFactory is required for creating an EntityParser");
		this.objectMapperFactory = objectMapperFactory;
	}

	public EntityMeta<?> parseEntity(Keyspace keyspace, Class<?> entityClass,
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled)
	{
		Map<Field, String> externalWideMaps = new HashMap<Field, String>();
		Map<Field, String> externalJoinWideMaps = new HashMap<Field, String>();

		ObjectMapper objectMapper = objectMapperFactory.getMapper(entityClass);
		Validator.validateNotNull(objectMapper, "No Jackson ObjectMapper found for entity '"
				+ entityClass.getCanonicalName() + "'");

		Validator.validateInstantiable(entityClass);
		String columnFamilyName = helper.inferColumnFamilyName(entityClass, entityClass.getName());
		Long serialVersionUID = helper.findSerialVersionUID(entityClass);
		boolean columnFamilyDirectMapping = entityClass
				.getAnnotation(info.archinnov.achilles.annotations.ColumnFamily.class) != null ? true
				: false;

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Void, ?> idMeta = null;

		List<Field> inheritedFields = helper.getInheritedPrivateFields(entityClass);

		for (Field field : inheritedFields)
		{
			boolean isWideMap = WideMap.class.isAssignableFrom(field.getType());
			if (filter.hasAnnotation(field, Id.class))
			{
				idMeta = parser.parse(entityClass, field, field.getName(), objectMapper);
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
					propertyMetas.put(propertyName,
							parser.parse(entityClass, field, propertyName, objectMapper));
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

				if ((StringUtils.isNotBlank(externalTableName) || columnFamilyDirectMapping)
						&& isWideMap)
				{
					externalJoinWideMaps.put(field, propertyName);
				}
				else
				{
					PropertyMeta<?, ?> joinPropertyMeta = parser.parse(entityClass, field,
							propertyName, objectMapper);
					propertyMetas.put(propertyName, joinPropertyMeta);
					joinPropertyMetaToBeFilled.put(joinPropertyMeta,
							joinPropertyMeta.getValueClass());
				}
			}

		}

		validateIdMeta(entityClass, idMeta);

		// Deferred external wide map fields parsing
		for (Entry<Field, String> entry : externalWideMaps.entrySet())
		{
			String propertyName = entry.getValue();
			propertyMetas.put(
					propertyName,
					parser.parseExternalWideMapProperty(keyspace, idMeta, entityClass,
							entry.getKey(), propertyName, objectMapper));
		}

		// Deferred external join wide map fields parsing
		for (Entry<Field, String> entry : externalJoinWideMaps.entrySet())
		{
			String propertyName = entry.getValue();

			PropertyMeta<Object, Object> joinPropertyMeta;
			if (columnFamilyDirectMapping)
			{
				joinPropertyMeta = parser.parseJoinWideMapPropertyForColumnFamily(keyspace, idMeta,
						entityClass, entry.getKey(), propertyName, columnFamilyName, objectMapper);

			}
			else
			{
				joinPropertyMeta = parser.parseExternalJoinWideMapProperty(keyspace, idMeta,
						entityClass, entry.getKey(), propertyName, objectMapper);

			}
			propertyMetas.put(propertyName, joinPropertyMeta);
			joinPropertyMetaToBeFilled.put(joinPropertyMeta, joinPropertyMeta.getValueClass());
		}

		validatePropertyMetas(entityClass, propertyMetas, columnFamilyDirectMapping);
		validateColumnFamily(entityClass, columnFamilyDirectMapping, propertyMetas);

		return entityMetaBuilder(idMeta).keyspace(keyspace)
				.className(entityClass.getCanonicalName()) //
				.columnFamilyName(columnFamilyName) //
				.serialVersionUID(serialVersionUID) //
				.propertyMetas(propertyMetas) //
				.columnFamilyDirectMapping(columnFamilyDirectMapping) //
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
			Map<String, PropertyMeta<?, ?>> propertyMetas, boolean columnFamilyDirectMapping)
	{
		if (propertyMetas.isEmpty())
		{
			throw new BeanMappingException(
					"The entity '"
							+ entityClass.getCanonicalName()
							+ "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");
		}
	}

	private void validateColumnFamily(Class<?> entityClass, boolean columnFamilyDirectMapping,
			Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		if (columnFamilyDirectMapping)
		{
			if (propertyMetas != null && propertyMetas.size() > 1)
			{
				throw new BeanMappingException("The ColumnFamily entity '"
						+ entityClass.getCanonicalName()
						+ "' should not have more than one property annotated with @Column");
			}

			PropertyType type = propertyMetas.entrySet().iterator().next().getValue().type();

			if (type != WIDE_MAP && type != EXTERNAL_JOIN_WIDE_MAP)
			{
				throw new BeanMappingException("The ColumnFamily entity '"
						+ entityClass.getCanonicalName()
						+ "' should have one and only one @Column/@JoinColumn of type WideMap");
			}
		}
	}

}
