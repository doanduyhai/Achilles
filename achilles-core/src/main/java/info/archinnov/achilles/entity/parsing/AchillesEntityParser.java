package info.archinnov.achilles.entity.parsing;

import static info.archinnov.achilles.entity.metadata.EntityMetaBuilder.entityMetaBuilder;
import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parsing.context.AchillesEntityParsingContext;
import info.archinnov.achilles.entity.parsing.context.AchillesPropertyParsingContext;
import info.archinnov.achilles.entity.parsing.validator.AchillesEntityParsingValidator;
import info.archinnov.achilles.helper.AchillesEntityIntrospector;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AchillesEntityParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesEntityParser
{
	private static final Logger log = LoggerFactory.getLogger(AchillesEntityParser.class);

	private AchillesEntityParsingValidator validator = new AchillesEntityParsingValidator();
	private AchillesPropertyParser parser = new AchillesPropertyParser();
	private AchillesJoinPropertyParser joinParser = new AchillesJoinPropertyParser();
	private AchillesPropertyFilter filter = new AchillesPropertyFilter();
	private AchillesEntityIntrospector introspector = new AchillesEntityIntrospector();

	public EntityMeta parseEntity(AchillesEntityParsingContext context)
	{
		log.debug("Parsing entity class {}", context.getCurrentEntityClass().getCanonicalName());

		Class<?> entityClass = context.getCurrentEntityClass();
		validateEntityAndGetObjectMapper(context);

		String columnFamilyName = introspector.inferColumnFamilyName(entityClass,
				entityClass.getName());
		Long serialVersionUID = introspector.findSerialVersionUID(entityClass);
		Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = introspector
				.findConsistencyLevels(entityClass, context.getConfigurableCLPolicy());

		context.setWideRow(entityClass.getAnnotation(WideRow.class) != null ? true : false);
		context.setCurrentConsistencyLevels(consistencyLevels);
		context.setCurrentColumnFamilyName(columnFamilyName);

		PropertyMeta<?, ?> idMeta = null;
		List<Field> inheritedFields = introspector.getInheritedPrivateFields(entityClass);
		for (Field field : inheritedFields)
		{
			AchillesPropertyParsingContext propertyContext = context.newPropertyContext(field);
			if (filter.hasAnnotation(field, Id.class))
			{
				propertyContext.setPrimaryKey(true);
				idMeta = parser.parse(propertyContext);
			}
			if (filter.hasAnnotation(field, EmbeddedId.class))
			{
				propertyContext.hasMultiKeyPrimaryKey(true);
				idMeta = parser.parse(propertyContext);
			}
			else if (filter.hasAnnotation(field, Column.class))
			{
				PropertyMeta<?, ?> propertyMeta = parser.parse(propertyContext);
				if (!propertyMeta.isLazy())
				{
					context.getEagerMetas().add(propertyMeta);
				}
			}
			else if (filter.hasAnnotation(field, JoinColumn.class))
			{
				propertyContext.setJoinColumn(true);
				joinParser.parseJoin(propertyContext);
			}
			else
			{
				log.trace("Un-mapped field {} of entity {} will not be managed by Achilles",
						field.getName(), context.getCurrentEntityClass().getCanonicalName());
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

		// Finish validation of property metas and wide row
		validator.validatePropertyMetas(context);
		validator.validateWideRows(context);

		EntityMeta entityMeta = entityMetaBuilder(idMeta)
				.entityClass(entityClass)
				.className(entityClass.getCanonicalName())
				.columnFamilyName(columnFamilyName)
				.serialVersionUID(serialVersionUID)
				.propertyMetas(context.getPropertyMetas())
				.eagerMetas(context.getEagerMetas())
				.wideRow(context.isWideRow())
				.consistencyLevels(context.getCurrentConsistencyLevels())
				.build();

		saveConsistencyLevel(context, columnFamilyName, consistencyLevels);

		log.trace("Entity meta built for entity class {} : {}", context
				.getCurrentEntityClass()
				.getCanonicalName(), entityMeta);
		return entityMeta;
	}

	public void fillJoinEntityMeta(AchillesEntityParsingContext context,
			Map<Class<?>, EntityMeta> entityMetaMap)
	{
		log.debug("Fill in join entity meta into property meta of join type");

		// Retrieve EntityMeta objects for join columns after entities parsing
		for (Entry<PropertyMeta<?, ?>, Class<?>> entry : context
				.getJoinPropertyMetaToBeFilled()
				.entrySet())
		{
			Class<?> clazz = entry.getValue();
			validator.validateJoinEntityExist(entityMetaMap, clazz);

			PropertyMeta<?, ?> propertyMeta = entry.getKey();
			EntityMeta joinEntityMeta = entityMetaMap.get(clazz);

			validator.validateJoinEntityNotWideRow(propertyMeta, joinEntityMeta);

			propertyMeta.getJoinProperties().setEntityMeta(joinEntityMeta);

			log.trace("Join property meta built for entity class {} : {}",
					clazz.getCanonicalName(), propertyMeta);
		}
	}

	private void validateEntityAndGetObjectMapper(AchillesEntityParsingContext context)
	{

		Class<?> entityClass = context.getCurrentEntityClass();
		log.debug("Validate entity {}", entityClass.getCanonicalName());

		Validator.validateSerializable(entityClass, "The entity '" + entityClass.getCanonicalName()
				+ "' should implements java.io.Serializable");
		Validator.validateInstantiable(entityClass);

		ObjectMapper objectMapper = context.getObjectMapperFactory().getMapper(entityClass);
		Validator.validateNotNull(objectMapper, "No Jackson ObjectMapper found for entity '"
				+ entityClass.getCanonicalName() + "'");

		log.debug("Set default object mapper {} for entity {}", objectMapper
				.getClass()
				.getCanonicalName(), entityClass.getCanonicalName());
		context.setCurrentObjectMapper(objectMapper);
	}

	private void processWideMap(AchillesEntityParsingContext context, PropertyMeta<?, ?> idMeta)
	{
		for (Entry<PropertyMeta<?, ?>, String> entry : context.getWideMaps().entrySet())
		{
			PropertyMeta<?, ?> externalWideMapMeta = entry.getKey();

			log.debug("Fill wide map meta {} of entity class {}", externalWideMapMeta
					.getPropertyName(), context.getCurrentEntityClass().getCanonicalName());

			parser.fillWideMap(context, idMeta, externalWideMapMeta, entry.getValue());
		}
	}

	private void processJoinWideMap(AchillesEntityParsingContext context, String columnFamilyName,
			PropertyMeta<?, ?> idMeta)
	{
		for (Entry<PropertyMeta<?, ?>, String> entry : context.getJoinWideMaps().entrySet())
		{
			PropertyMeta<?, ?> joinExternalWideMapMeta = entry.getKey();

			log.debug("Fill join wide map meta {} of entity class {}", joinExternalWideMapMeta
					.getPropertyName(), context.getCurrentEntityClass().getCanonicalName());

			joinParser.fillJoinWideMap(context, idMeta, joinExternalWideMapMeta, entry.getValue());
		}
	}

	private void completeCounterPropertyMeta(AchillesEntityParsingContext context,
			PropertyMeta<?, ?> idMeta)
	{
		for (PropertyMeta<?, ?> counterMeta : context.getCounterMetas())
		{

			log.debug("Add id Meta {} to counter meta {} of entity class {}", idMeta
					.getPropertyName(), counterMeta.getPropertyName(), context
					.getCurrentEntityClass()
					.getCanonicalName());

			counterMeta.getCounterProperties().setIdMeta(idMeta);
		}
	}

	private void saveConsistencyLevel(AchillesEntityParsingContext context,
			String columnFamilyName, Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		log.debug("Set default read/write consistency levels {} / {} for column family {}",
				consistencyLevels.left.name(), consistencyLevels.right.name(), columnFamilyName);

		context.getConfigurableCLPolicy().setConsistencyLevelForRead(consistencyLevels.left,
				columnFamilyName);
		context.getConfigurableCLPolicy().setConsistencyLevelForWrite(consistencyLevels.right,
				columnFamilyName);
	}

}
