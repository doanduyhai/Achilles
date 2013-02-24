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
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

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

	public PropertyMeta<?, ?> parseJoin(Map<String, PropertyMeta<?, ?>> propertyMetas, //
			Map<Field, String> externalJoinWideMaps, //
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled, //
			Class<?> entityClass, Field field, //
			ObjectMapper objectMapper)
	{
		JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
		String externalTableName = field.getAnnotation(JoinColumn.class).table();
		boolean isWideMap = WideMap.class.isAssignableFrom(field.getType());
		boolean columnFamilyDirectMapping = entityClass
				.getAnnotation(info.archinnov.achilles.annotations.ColumnFamily.class) != null ? true
				: false;

		String propertyName = StringUtils.isNotBlank(joinColumn.name()) ? joinColumn.name() : field
				.getName();
		Validator.validateFalse(propertyMetas.containsKey(propertyName),
				"The property '" + propertyName + "' is already used for the entity '"
						+ entityClass.getCanonicalName() + "'");

		PropertyMeta<?, ?> joinPropertyMeta = null;
		if ((StringUtils.isNotBlank(externalTableName) || columnFamilyDirectMapping) && isWideMap)
		{
			externalJoinWideMaps.put(field, propertyName);
		}
		else
		{
			joinPropertyMeta = this.parser.parse(propertyMetas, externalJoinWideMaps, entityClass,
					field, true, objectMapper);

			JoinProperties joinProperties = findCascadeType(entityClass.getCanonicalName(), field);
			joinPropertyMeta.setJoinProperties(joinProperties);

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

			propertyMetas.put(propertyName, joinPropertyMeta);
			joinPropertyMetaToBeFilled.put(joinPropertyMeta, joinPropertyMeta.getValueClass());
		}

		return joinPropertyMeta;
	}

	public <ID> PropertyMeta<?, ?> parseExternalJoinWideMapProperty(Keyspace keyspace,
			PropertyMeta<Void, ID> idMeta, Class<?> entityClass, Field field, String propertyName,
			String columnFamilyName, ObjectMapper objectMapper)
	{

		boolean columnFamilyDirectMapping = entityClass
				.getAnnotation(info.archinnov.achilles.annotations.ColumnFamily.class) != null ? true
				: false;

		PropertyMeta<?, ?> joinPropertyMeta = this.parser.parseWideMapProperty(entityClass, field,
				propertyName, objectMapper);

		JoinProperties joinProperties = findCascadeType(entityClass.getCanonicalName(), field);
		joinPropertyMeta.setJoinProperties(joinProperties);
		joinPropertyMeta.setType(EXTERNAL_JOIN_WIDE_MAP);

		if (columnFamilyDirectMapping)
		{
			joinPropertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(
					ColumnFamilyHelper.normalizerAndValidateColumnFamilyName(columnFamilyName),
					null, idMeta.getValueSerializer()));
		}
		else
		{
			String externalTableName = field.getAnnotation(JoinColumn.class).table();
			joinPropertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(
					externalTableName, null, idMeta.getValueSerializer()));
		}

		return joinPropertyMeta;
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
