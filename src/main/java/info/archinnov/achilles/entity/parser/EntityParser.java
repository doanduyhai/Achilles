package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import info.archinnov.achilles.annotations.ColumnFamily;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.validator.EntityParsingValidator;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * EntityParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParser
{
	private EntityParsingValidator validator = new EntityParsingValidator();
	private PropertyParser parser = new PropertyParser();
	private JoinPropertyParser joinParser = new JoinPropertyParser();
	private PropertyFilter filter = new PropertyFilter();
	private EntityHelper helper = new EntityHelper();

	@SuppressWarnings("unchecked")
	public <ID> EntityMeta<ID> parseEntity(EntityParsingContext context)
	{
		Class<?> entityClass = context.getCurrentEntityClass();
		validateEntityAndGetObjectMapper(context);

		String columnFamilyName = helper.inferColumnFamilyName(entityClass, entityClass.getName());
		Long serialVersionUID = helper.findSerialVersionUID(entityClass);
		Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = helper
				.findConsistencyLevels(entityClass);

		context.setColumnFamilyDirectMapping(entityClass.getAnnotation(ColumnFamily.class) != null ? true
				: false);
		context.setCurrentConsistencyLevels(consistencyLevels);
		context.setCurrentColumnFamilyName(columnFamilyName);

		PropertyMeta<Void, ID> idMeta = null;
		List<Field> inheritedFields = helper.getInheritedPrivateFields(entityClass);
		for (Field field : inheritedFields)
		{
			PropertyParsingContext propertyContext = context.newPropertyContext(field);
			if (filter.hasAnnotation(field, Id.class))
			{
				propertyContext.setPrimaryKey(true);
				idMeta = (PropertyMeta<Void, ID>) parser.parse(propertyContext);
			}
			else if (filter.hasAnnotation(field, Column.class))
			{
				parser.parse(propertyContext);
			}
			else if (filter.hasAnnotation(field, JoinColumn.class))
			{
				propertyContext.setJoinColumn(true);
				joinParser.parseJoin(propertyContext);
			}

		}

		// First validate id meta
		validator.validateHasIdMeta(entityClass, idMeta);

		// Deferred external wide map fields parsing
		processExternalWideMap(context, idMeta);

		// Deferred external join wide map fields parsing
		processExternalJoinWideMap(context, columnFamilyName, idMeta);

		// Deferred counter property meta completion
		completeCounterPropertyMeta(context, idMeta);

		// Finish validation of property metas and column family direct mappings
		validator.validatePropertyMetas(context);
		validator.validateColumnFamilyDirectMappings(context);

		EntityMeta<ID> entityMeta = entityMetaBuilder((PropertyMeta<Void, ID>) idMeta)
				.keyspace(context.getKeyspace()) //
				.className(entityClass.getCanonicalName()) //
				.columnFamilyName(columnFamilyName) //
				.serialVersionUID(serialVersionUID) //
				.propertyMetas(context.getPropertyMetas()) //
				.columnFamilyDirectMapping(context.isColumnFamilyDirectMapping()) //
				.consistencyLevels(context.getCurrentConsistencyLevels()) //
				.build();

		buildDao(context, columnFamilyName, idMeta);
		saveConsistencyLevel(context, columnFamilyName, consistencyLevels);

		return entityMeta;
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID> void fillJoinEntityMeta(EntityParsingContext context,
			Map<Class<?>, EntityMeta<?>> entityMetaMap)
	{
		// Retrieve EntityMeta objects for join columns after entities parsing
		for (Entry<PropertyMeta<?, ?>, Class<?>> entry : context.getJoinPropertyMetaToBeFilled()
				.entrySet())
		{
			Class<?> clazz = entry.getValue();
			validator.validateJoinEntityExist(entityMetaMap, clazz);

			PropertyMeta<?, ?> propertyMeta = entry.getKey();
			EntityMeta<JOIN_ID> joinEntityMeta = (EntityMeta<JOIN_ID>) entityMetaMap.get(clazz);

			validator.validateJoinEntityNotDirectCFMapping(joinEntityMeta);

			propertyMeta.getJoinProperties().setEntityMeta(joinEntityMeta);
			if (propertyMeta.type().isExternal())
			{
				ExternalWideMapProperties<ID> externalWideMapProperties = (ExternalWideMapProperties<ID>) propertyMeta
						.getExternalWideMapProperties();

				GenericCompositeDao<ID, JOIN_ID> joinDao = new GenericCompositeDao<ID, JOIN_ID>(
						context.getCluster(), //
						context.getKeyspace(), //
						externalWideMapProperties.getIdSerializer(), //
						joinEntityMeta.getIdSerializer(), //
						externalWideMapProperties.getExternalColumnFamilyName(),//
						context.getConfigurableCLPolicy());

				context.getColumnFamilyDaosMap().put(
						externalWideMapProperties.getExternalColumnFamilyName(), joinDao);
			}
		}
	}

	private void validateEntityAndGetObjectMapper(EntityParsingContext context)
	{
		Class<?> entityClass = context.getCurrentEntityClass();
		Validator.validateSerializable(entityClass, "The entity '" + entityClass.getCanonicalName()
				+ "' should implements java.io.Serializable");
		Validator.validateInstantiable(entityClass);
		ObjectMapper objectMapper = context.getObjectMapperFactory().getMapper(entityClass);
		Validator.validateNotNull(objectMapper, "No Jackson ObjectMapper found for entity '"
				+ entityClass.getCanonicalName() + "'");

		context.setCurrentObjectMapper(objectMapper);
	}

	private void processExternalWideMap(EntityParsingContext context, PropertyMeta<Void, ?> idMeta)
	{
		for (Entry<PropertyMeta<?, ?>, String> entry : context.getExternalWideMaps().entrySet())
		{
			PropertyMeta<?, ?> externalWideMapMeta = entry.getKey();
			String externalTableName = entry.getValue();
			parser.fillExternalWideMap(context, idMeta, externalWideMapMeta, externalTableName);
		}
	}

	private void processExternalJoinWideMap(EntityParsingContext context, String columnFamilyName,
			PropertyMeta<Void, ?> idMeta)
	{
		for (Entry<PropertyMeta<?, ?>, String> entry : context.getJoinExternalWideMaps().entrySet())
		{
			PropertyMeta<?, ?> joinExternalWideMapMeta = entry.getKey();
			String externalTableName;

			externalTableName = entry.getValue();
			joinParser.fillExternalJoinWideMap(context, idMeta, joinExternalWideMapMeta,
					externalTableName);
		}
	}

	private void completeCounterPropertyMeta(EntityParsingContext context,
			PropertyMeta<Void, ?> idMeta)
	{
		for (PropertyMeta<?, ?> counterMeta : context.getCounterMetas())
		{
			counterMeta.getCounterProperties().setIdMeta(idMeta);
		}
	}

	private <ID, K, V> void buildDao(EntityParsingContext context, String columnFamilyName,
			PropertyMeta<Void, ID> idMeta)
	{
		GenericDynamicCompositeDao<ID> entityDao = new GenericDynamicCompositeDao<ID>(
				context.getCluster(), //
				context.getKeyspace(), //
				idMeta.getValueSerializer(), //
				columnFamilyName, //
				context.getConfigurableCLPolicy());
	
		context.getEntityDaosMap().put(columnFamilyName, entityDao);
	}

	private void saveConsistencyLevel(EntityParsingContext context, String columnFamilyName,
			Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		context.getConfigurableCLPolicy().setConsistencyLevelForRead(
				consistencyLevels.left.getHectorLevel(), columnFamilyName);
		context.getConfigurableCLPolicy().setConsistencyLevelForWrite(
				consistencyLevels.right.getHectorLevel(), columnFamilyName);
	}

}
