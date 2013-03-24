package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import info.archinnov.achilles.columnFamily.ColumnFamilyHelper;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.lang.reflect.Field;
import java.util.Arrays;

import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

/**
 * JoinPropertyParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinPropertyParser
{
	private PropertyFilter filter = new PropertyFilter();
	private PropertyParser parser = new PropertyParser();

	public PropertyMeta<?, ?> parseJoin(PropertyParsingContext context)
	{
		Class<?> entityClass = context.getCurrentEntityClass();
		Field field = context.getCurrentField();

		PropertyMeta<?, ?> joinPropertyMeta = this.parser.parse(context);

		JoinProperties joinProperties = findCascadeType(entityClass.getCanonicalName(), field);
		joinPropertyMeta.setJoinProperties(joinProperties);

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

	public <ID> void fillExternalJoinWideMap(EntityParsingContext context,
			PropertyMeta<Void, ID> idMeta, PropertyMeta<?, ?> joinPropertyMeta,
			String externalTableName)
	{
		joinPropertyMeta.setType(EXTERNAL_JOIN_WIDE_MAP);

		joinPropertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(
				ColumnFamilyHelper.normalizerAndValidateColumnFamilyName(externalTableName), idMeta
						.getValueSerializer()));
		context.getPropertyMetas().put(joinPropertyMeta.getPropertyName(), joinPropertyMeta);
		context.getJoinPropertyMetaToBeFilled().put(joinPropertyMeta,
				joinPropertyMeta.getValueClass());
	}

	private JoinProperties findCascadeType(String entityFQN, Field field)
	{
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
		return joinProperties;
	}
}
