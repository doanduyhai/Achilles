package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.manager.ThriftEntityManagerFactoryImpl.joinPropertyMetaToBeFilledTL;
import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.parser.EntityParser.entityClassTL;
import static info.archinnov.achilles.entity.parser.EntityParser.joinExternalWideMapTL;
import static info.archinnov.achilles.entity.parser.EntityParser.propertyMetasTL;
import info.archinnov.achilles.columnFamily.ColumnFamilyHelper;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Field;
import java.util.Arrays;

import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;

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

	public PropertyMeta<?, ?> parseJoin(Field field)
	{
		Class<?> entityClass = entityClassTL.get();

		JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
		String externalTableName = field.getAnnotation(JoinColumn.class).table();
		boolean isWideMap = WideMap.class.isAssignableFrom(field.getType());
		boolean columnFamilyDirectMapping = entityClass
				.getAnnotation(info.archinnov.achilles.annotations.ColumnFamily.class) != null ? true
				: false;
		boolean isExternal = StringUtils.isNotBlank(externalTableName);

		String propertyName = StringUtils.isNotBlank(joinColumn.name()) ? joinColumn.name() : field
				.getName();
		Validator.validateFalse(propertyMetasTL.get().containsKey(propertyName),
				"The property '" + propertyName + "' is already used for the entity '"
						+ entityClass.getCanonicalName() + "'");

		PropertyMeta<?, ?> joinPropertyMeta = null;
		joinPropertyMeta = this.parser.parse(field, true);

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

		propertyMetasTL.get().put(propertyName, joinPropertyMeta);

		if ((isExternal || columnFamilyDirectMapping) && isWideMap)
		{
			joinExternalWideMapTL.get().put(joinPropertyMeta, externalTableName);
		}
		joinPropertyMetaToBeFilledTL.get().put(joinPropertyMeta, joinPropertyMeta.getValueClass());

		return joinPropertyMeta;
	}

	public <ID> void fillExternalJoinWideMap(Keyspace keyspace, PropertyMeta<Void, ID> idMeta,
			PropertyMeta<?, ?> joinPropertyMeta, String externalTableName)
	{
		joinPropertyMeta.setType(EXTERNAL_JOIN_WIDE_MAP);

		joinPropertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(
				ColumnFamilyHelper.normalizerAndValidateColumnFamilyName(externalTableName), null,
				idMeta.getValueSerializer()));
		propertyMetasTL.get().put(joinPropertyMeta.getPropertyName(), joinPropertyMeta);
		joinPropertyMetaToBeFilledTL.get().put(joinPropertyMeta, joinPropertyMeta.getValueClass());
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
