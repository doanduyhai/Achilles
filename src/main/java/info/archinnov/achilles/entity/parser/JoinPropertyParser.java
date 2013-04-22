package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import info.archinnov.achilles.columnFamily.ThriftColumnFamilyHelper;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.entity.parser.context.PropertyParsingContext;

import java.lang.reflect.Field;
import java.util.Arrays;

import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JoinPropertyParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinPropertyParser
{
	private static final Logger log = LoggerFactory.getLogger(JoinPropertyParser.class);

	private PropertyFilter filter = new PropertyFilter();
	private PropertyParser parser = new PropertyParser();

	public PropertyMeta<?, ?> parseJoin(PropertyParsingContext context)
	{

		Class<?> entityClass = context.getCurrentEntityClass();
		Field field = context.getCurrentField();

		PropertyMeta<?, ?> joinPropertyMeta = this.parser.parse(context);

		log.debug("Parsing join property meta {} for entity {}",
				joinPropertyMeta.getPropertyName(), context.getCurrentEntityClass()
						.getCanonicalName());

		joinPropertyMeta.setJoinProperties(findCascadeType(entityClass.getCanonicalName(), field));

		// Override each type by their JOIN type counterpart
		switch (joinPropertyMeta.type())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				joinPropertyMeta.setType(JOIN_SIMPLE);
				break;
			case LIST:
			case LAZY_LIST:
				joinPropertyMeta.setType(JOIN_LIST);
				break;
			case SET:
			case LAZY_SET:
				joinPropertyMeta.setType(JOIN_SET);
				break;
			case MAP:
			case LAZY_MAP:
				joinPropertyMeta.setType(JOIN_MAP);
				break;
			case WIDE_MAP:
				joinPropertyMeta.setType(JOIN_WIDE_MAP);
				break;
			default:
				break;
		}

		context.getJoinPropertyMetaToBeFilled().put(joinPropertyMeta,
				joinPropertyMeta.getValueClass());

		return joinPropertyMeta;
	}

	public <ID> void fillJoinWideMap(EntityParsingContext context, PropertyMeta<Void, ID> idMeta,
			PropertyMeta<?, ?> joinPropertyMeta, String externalTableName)
	{
		log.debug("Filling join wide map meta {} of entity class {} with id meta {} info",
				joinPropertyMeta.getPropertyName(), context.getCurrentEntityClass()
						.getCanonicalName(), idMeta.getPropertyName());

		joinPropertyMeta.setExternalCfName(ThriftColumnFamilyHelper
				.normalizerAndValidateColumnFamilyName(externalTableName));
		joinPropertyMeta.setIdSerializer(idMeta.getValueSerializer());
		context.getPropertyMetas().put(joinPropertyMeta.getPropertyName(), joinPropertyMeta);
		context.getJoinPropertyMetaToBeFilled().put(joinPropertyMeta,
				joinPropertyMeta.getValueClass());

		log.trace("Complete join wide map property {} of entity class {} : {}", joinPropertyMeta
				.getPropertyName(), context.getCurrentEntityClass().getCanonicalName(),
				joinPropertyMeta);
	}

	private JoinProperties findCascadeType(String entityFQN, Field field)
	{
		log.trace("Find cascade type for property {} of entity class {}", field.getName(), field
				.getDeclaringClass().getCanonicalName());

		JoinProperties joinProperties = new JoinProperties();

		if (filter.hasAnnotation(field, OneToOne.class))
		{
			OneToOne oneToOne = field.getAnnotation(OneToOne.class);
			joinProperties.addCascadeType(Arrays.asList(oneToOne.cascade()));
		}
		else if (filter.hasAnnotation(field, OneToMany.class))
		{
			OneToMany oneToMany = field.getAnnotation(OneToMany.class);
			joinProperties.addCascadeType(Arrays.asList(oneToMany.cascade()));
		}
		if (filter.hasAnnotation(field, ManyToOne.class))
		{
			ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);
			joinProperties.addCascadeType(Arrays.asList(manyToOne.cascade()));
		}
		else if (filter.hasAnnotation(field, ManyToMany.class))
		{
			ManyToMany manyToMany = field.getAnnotation(ManyToMany.class);
			joinProperties.addCascadeType(Arrays.asList(manyToMany.cascade()));
		}

		log.trace("Built join properties for property {} of entity class {}", joinProperties,
				field.getName(), field.getDeclaringClass().getCanonicalName());
		return joinProperties;
	}
}
