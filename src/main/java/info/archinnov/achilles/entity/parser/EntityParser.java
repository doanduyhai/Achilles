package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import info.archinnov.achilles.annotations.ColumnFamily;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.entity.parser.context.PropertyParsingContext;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParser
{
	private static final Logger log = LoggerFactory.getLogger(EntityParser.class);

	private EntityParsingValidator validator = new EntityParsingValidator();
	private PropertyParser parser = new PropertyParser();
	private JoinPropertyParser joinParser = new JoinPropertyParser();
	private PropertyFilter filter = new PropertyFilter();
	private EntityIntrospector introspector = new EntityIntrospector();

	@SuppressWarnings("unchecked")
	public <ID> EntityMeta<ID> parseEntity(EntityParsingContext context)
	{
		log.debug("Parsing entity class {}", context.getCurrentEntityClass().getCanonicalName());

		Class<?> entityClass = context.getCurrentEntityClass();
		validateEntityAndGetObjectMapper(context);

		String columnFamilyName = introspector.inferColumnFamilyName(entityClass,
				entityClass.getName());
		Long serialVersionUID = introspector.findSerialVersionUID(entityClass);
		Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = introspector
				.findConsistencyLevels(entityClass, context.getConfigurableCLPolicy());

		context.setColumnFamilyDirectMapping(entityClass.getAnnotation(ColumnFamily.class) != null ? true
				: false);
		context.setCurrentConsistencyLevels(consistencyLevels);
		context.setCurrentColumnFamilyName(columnFamilyName);

		PropertyMeta<Void, ID> idMeta = null;
		List<Field> inheritedFields = introspector.getInheritedPrivateFields(entityClass);
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
		processWideMap(context, idMeta);

		// Deferred external join wide map fields parsing
		processJoinWideMap(context, columnFamilyName, idMeta);

		// Deferred counter property meta completion
		completeCounterPropertyMeta(context, idMeta);

		// Finish validation of property metas and column family direct mappings
		validator.validatePropertyMetas(context);
		validator.validateColumnFamilyDirectMappings(context);

		EntityMeta<ID> entityMeta = entityMetaBuilder((PropertyMeta<Void, ID>) idMeta)
				.className(entityClass.getCanonicalName()) //
				.columnFamilyName(columnFamilyName) //
				.serialVersionUID(serialVersionUID) //
				.propertyMetas(context.getPropertyMetas()) //
				.columnFamilyDirectMapping(context.isColumnFamilyDirectMapping()) //
				.consistencyLevels(context.getCurrentConsistencyLevels()) //
				.build();

		// buildDao(context, columnFamilyName, idMeta);
		saveConsistencyLevel(context, columnFamilyName, consistencyLevels);

		log.trace("Entity meta built for entity class {} : {}", context.getCurrentEntityClass()
				.getCanonicalName(), entityMeta);
		return entityMeta;
	}

	@SuppressWarnings("unchecked")
	public <ID, JOIN_ID> void fillJoinEntityMeta(EntityParsingContext context,
			Map<Class<?>, EntityMeta<?>> entityMetaMap)
	{
		log.debug("Fill in join entity meta into property meta of join type");

		// Retrieve EntityMeta objects for join columns after entities parsing
		for (Entry<PropertyMeta<?, ?>, Class<?>> entry : context.getJoinPropertyMetaToBeFilled()
				.entrySet())
		{
			Class<?> clazz = entry.getValue();
			validator.validateJoinEntityExist(entityMetaMap, clazz);

			PropertyMeta<?, ?> propertyMeta = entry.getKey();
			EntityMeta<JOIN_ID> joinEntityMeta = (EntityMeta<JOIN_ID>) entityMetaMap.get(clazz);

			validator.validateJoinEntityNotDirectCFMapping(propertyMeta, joinEntityMeta);

			propertyMeta.getJoinProperties().setEntityMeta(joinEntityMeta);

			// TODO
			// if (propertyMeta.type().isWideMap())
			// {
			//
			// GenericColumnFamilyDao<ID, JOIN_ID> joinDao = new GenericColumnFamilyDao<ID, JOIN_ID>(
			// context.getCluster(), //
			// context.getKeyspace(), //
			// (Serializer<ID>) propertyMeta.getIdSerializer(), //
			// joinEntityMeta.getIdSerializer(), //
			// propertyMeta.getExternalCFName(),//
			// context.getConfigurableCLPolicy());
			//
			// log.debug("Building join dao for column family {}",
			// propertyMeta.getExternalCFName());
			//
			// context.getColumnFamilyDaosMap().put(propertyMeta.getExternalCFName(), joinDao);
			// }

			log.trace("Join property meta built for entity class {} : {}",
					clazz.getCanonicalName(), propertyMeta);
		}
	}

	private void validateEntityAndGetObjectMapper(EntityParsingContext context)
	{

		Class<?> entityClass = context.getCurrentEntityClass();
		log.debug("Validate entity {}", entityClass.getCanonicalName());

		Validator.validateSerializable(entityClass, "The entity '" + entityClass.getCanonicalName()
				+ "' should implements java.io.Serializable");
		Validator.validateInstantiable(entityClass);

		ObjectMapper objectMapper = context.getObjectMapperFactory().getMapper(entityClass);
		Validator.validateNotNull(objectMapper, "No Jackson ObjectMapper found for entity '"
				+ entityClass.getCanonicalName() + "'");

		log.debug("Set default object mapper {} for entity {}", objectMapper.getClass()
				.getCanonicalName(), entityClass.getCanonicalName());
		context.setCurrentObjectMapper(objectMapper);
	}

	private void processWideMap(EntityParsingContext context, PropertyMeta<Void, ?> idMeta)
	{
		for (Entry<PropertyMeta<?, ?>, String> entry : context.getWideMaps().entrySet())
		{
			PropertyMeta<?, ?> externalWideMapMeta = entry.getKey();

			log.debug("Fill wide map meta {} of entity class {}", externalWideMapMeta
					.getPropertyName(), context.getCurrentEntityClass().getCanonicalName());

			parser.fillWideMap(context, idMeta, externalWideMapMeta, entry.getValue());
		}
	}

	private void processJoinWideMap(EntityParsingContext context, String columnFamilyName,
			PropertyMeta<Void, ?> idMeta)
	{
		for (Entry<PropertyMeta<?, ?>, String> entry : context.getJoinWideMaps().entrySet())
		{
			PropertyMeta<?, ?> joinExternalWideMapMeta = entry.getKey();

			log.debug("Fill join wide map meta {} of entity class {}", joinExternalWideMapMeta
					.getPropertyName(), context.getCurrentEntityClass().getCanonicalName());

			joinParser.fillJoinWideMap(context, idMeta, joinExternalWideMapMeta, entry.getValue());
		}
	}

	private void completeCounterPropertyMeta(EntityParsingContext context,
			PropertyMeta<Void, ?> idMeta)
	{
		for (PropertyMeta<?, ?> counterMeta : context.getCounterMetas())
		{

			log.debug("Add id Meta {} to counter meta {} of entity class {}", idMeta
					.getPropertyName(), counterMeta.getPropertyName(), context
					.getCurrentEntityClass().getCanonicalName());

			counterMeta.getCounterProperties().setIdMeta(idMeta);
		}
	}

	// TODO
	// private <ID, K, V> void buildDao(EntityParsingContext context, String columnFamilyName,
	// PropertyMeta<Void, ID> idMeta)
	// {
	// GenericEntityDao<ID> entityDao = new GenericEntityDao<ID>(context.getCluster(), //
	// context.getKeyspace(), //
	// idMeta.getValueSerializer(), //
	// columnFamilyName, //
	// context.getConfigurableCLPolicy());
	//
	// log.debug("Build entity dao for column family {}", columnFamilyName);
	//
	// context.getEntityDaosMap().put(columnFamilyName, entityDao);
	// }

	private void saveConsistencyLevel(EntityParsingContext context, String columnFamilyName,
			Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		log.debug("Set default read/write consistency levels {} / {} for column family {}",
				consistencyLevels.left.name(), consistencyLevels.right.name(), columnFamilyName);

		context.getConfigurableCLPolicy().setConsistencyLevelForRead(
				consistencyLevels.left.getHectorLevel(), columnFamilyName);
		context.getConfigurableCLPolicy().setConsistencyLevelForWrite(
				consistencyLevels.right.getHectorLevel(), columnFamilyName);
	}

}
