package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
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

import org.codehaus.jackson.map.ObjectMapper;

/**
 * EntityParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParser
{
	private PropertyParser parser = new PropertyParser();
	private JoinPropertyParser joinParser = new JoinPropertyParser();
	private PropertyFilter filter = new PropertyFilter();
	private EntityHelper helper = new EntityHelper();
	private ObjectMapperFactory objectMapperFactory;

	public EntityParser(ObjectMapperFactory objectMapperFactory) {
		Validator.validateNotNull(objectMapperFactory,
				"A non null ObjectMapperFactory is required for creating an EntityParser");
		this.objectMapperFactory = objectMapperFactory;
	}

	@SuppressWarnings("unchecked")
	public Pair<EntityMeta<?>, Map<PropertyMeta<?, ?>, Class<?>>> parseEntity(Keyspace keyspace,
			CounterDao counterDao, Class<?> entityClass)
	{

		Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
		Validator.validateSerializable(entityClass, "The entity '" + entityClass.getCanonicalName()
				+ "' should implements java.io.Serializable");
		Validator.validateInstantiable(entityClass);
		ObjectMapper objectMapper = objectMapperFactory.getMapper(entityClass);
		Validator.validateNotNull(objectMapper, "No Jackson ObjectMapper found for entity '"
				+ entityClass.getCanonicalName() + "'");

		Map<Field, String> externalWideMaps = new HashMap<Field, String>();
		Map<Field, String> externalJoinWideMaps = new HashMap<Field, String>();
		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

		String columnFamilyName = helper.inferColumnFamilyName(entityClass, entityClass.getName());
		Long serialVersionUID = helper.findSerialVersionUID(entityClass);
		boolean columnFamilyDirectMapping = entityClass
				.getAnnotation(info.archinnov.achilles.annotations.ColumnFamily.class) != null ? true
				: false;

		PropertyMeta<Void, ?> idMeta = null;

		List<Field> inheritedFields = helper.getInheritedPrivateFields(entityClass);

		for (Field field : inheritedFields)
		{
			if (filter.hasAnnotation(field, Id.class))
			{
				idMeta = parser.parseSimpleProperty(entityClass, field, field.getName(),
						objectMapper, entityClass.getCanonicalName(), counterDao);
			}
			else if (filter.hasAnnotation(field, Column.class))
			{
				parser.parse(propertyMetas, externalWideMaps, entityClass, field, false,
						objectMapper, counterDao);
			}
			else if (filter.hasAnnotation(field, JoinColumn.class))
			{
				joinParser.parseJoin(propertyMetas, externalJoinWideMaps,
						joinPropertyMetaToBeFilled, entityClass, field, objectMapper);
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

			PropertyMeta<?, ?> joinPropertyMeta;
			joinPropertyMeta = joinParser.parseExternalJoinWideMapProperty(keyspace, idMeta,
					entityClass, entry.getKey(), propertyName, columnFamilyName, objectMapper);
			propertyMetas.put(propertyName, joinPropertyMeta);
			joinPropertyMetaToBeFilled.put(joinPropertyMeta, joinPropertyMeta.getValueClass());
		}

		validatePropertyMetas(entityClass, propertyMetas, columnFamilyDirectMapping);
		validateColumnFamily(entityClass, columnFamilyDirectMapping, propertyMetas);

		return new Pair<EntityMeta<?>, Map<PropertyMeta<?, ?>, Class<?>>>(//
				entityMetaBuilder((PropertyMeta<Void, Object>) idMeta).keyspace(keyspace)
						.className(entityClass.getCanonicalName()) //
						.columnFamilyName(columnFamilyName) //
						.serialVersionUID(serialVersionUID) //
						.propertyMetas(propertyMetas) //
						.columnFamilyDirectMapping(columnFamilyDirectMapping) //
						.build(), //
				joinPropertyMetaToBeFilled);
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID> void fillJoinEntityMeta(
			Keyspace keyspace, //
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled,
			Map<Class<?>, EntityMeta<?>> entityMetaMap)
	{
		// Retrieve EntityMeta objects for join columns after entities parsing
		for (Entry<PropertyMeta<?, ?>, Class<?>> entry : joinPropertyMetaToBeFilled.entrySet())
		{
			Class<?> clazz = entry.getValue();
			if (entityMetaMap.containsKey(clazz))
			{
				PropertyMeta<?, ?> propertyMeta = entry.getKey();
				EntityMeta<JOIN_ID> joinEntityMeta = (EntityMeta<JOIN_ID>) entityMetaMap.get(clazz);

				if (joinEntityMeta.isColumnFamilyDirectMapping())
				{
					throw new BeanMappingException("The entity '" + clazz.getCanonicalName()
							+ "' is a direct Column Family mapping and cannot be a join entity");
				}

				propertyMeta.getJoinProperties().setEntityMeta(joinEntityMeta);
				if (propertyMeta.type().isExternal())
				{
					ExternalWideMapProperties<ID> externalWideMapProperties = (ExternalWideMapProperties<ID>) propertyMeta
							.getExternalWideMapProperties();

					externalWideMapProperties.setExternalWideMapDao( //
							new GenericCompositeDao<ID, JOIN_ID>(keyspace, //
									externalWideMapProperties.getIdSerializer(), //
									joinEntityMeta.getIdSerializer(), //
									externalWideMapProperties.getExternalColumnFamilyName()));
				}
			}
			else
			{
				throw new BeanMappingException("Cannot find mapping for join entity '"
						+ clazz.getCanonicalName() + "'");
			}
		}
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
