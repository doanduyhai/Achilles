package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.annotations.WideRow;
import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.validation.Validator;

public class EntityParser
{
	private PropertyParser parser = new PropertyParser();
	private PropertyFilter filter = new PropertyFilter();
	private EntityHelper helper = new EntityHelper();

	public EntityMeta<?> parseEntity(Keyspace keyspace, Class<?> entityClass,
			Map<Class<?>, EntityMeta<?>> entityMetaMap, ColumnFamilyHelper columnFamilyHelper,
			boolean forceColumnFamilyCreation)
	{
		Validator.validateInstantiable(entityClass);
		String columnFamily = helper.inferColumnFamilyName(entityClass, entityClass.getCanonicalName());
		Long serialVersionUID = helper.findSerialVersionUID(entityClass);
		boolean wideRow = entityClass.getAnnotation(WideRow.class) != null ? true : false;

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Void, ?> idMeta = null;

		List<Field> inheritedFields = helper.getInheritedPrivateFields(entityClass);

		for (Field field : inheritedFields)
		{
			if (filter.hasAnnotation(field, Id.class))
			{
				idMeta = parser.parse(entityClass, field, field.getName());
			}

			else if (filter.hasAnnotation(field, Column.class))
			{
				Column column = field.getAnnotation(Column.class);
				String propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field
						.getName();
				propertyMetas.put(propertyName, parser.parse(entityClass, field, propertyName));
			}
			else if (filter.hasAnnotation(field, JoinColumn.class))
			{

			}

		}

		validateIdMeta(entityClass, idMeta);
		validatePropertyMetas(entityClass, propertyMetas);
		validateWideRow(entityClass, wideRow, propertyMetas);

		return entityMetaBuilder(idMeta).keyspace(keyspace).canonicalClassName(entityClass.getCanonicalName())
				.columnFamilyName(columnFamily).serialVersionUID(serialVersionUID)
				.propertyMetas(propertyMetas).wideRow(wideRow).build();
	}

	private void validateIdMeta(Class<?> entityClass, PropertyMeta<Void, ?> idMeta)
	{
		if (idMeta == null)
		{
			throw new IncorrectTypeException("The entity '" + entityClass.getCanonicalName()
					+ "' should have at least one field with javax.persistence.Id annotation");
		}
	}

	private void validatePropertyMetas(Class<?> entityClass,
			Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		if (propertyMetas.isEmpty())
		{
			throw new IncorrectTypeException(
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
			Validator.validateSize(propertyMetas, 1,
					"The WideRow entity '" + entityClass.getCanonicalName()
							+ "' should not have more than one property annotated with @Column");

			PropertyType type = propertyMetas.entrySet().iterator().next().getValue()
					.propertyType();

			Validator.validateTrue(type == PropertyType.WIDE_MAP, "The WideRow entity '"
					+ entityClass.getCanonicalName() + "' should have a @Column of type WideMap");
		}
	}

}
