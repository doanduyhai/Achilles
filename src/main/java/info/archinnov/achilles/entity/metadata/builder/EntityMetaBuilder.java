package info.archinnov.achilles.entity.metadata.builder;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Pair;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityMetaBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMetaBuilder
{
	private static final Logger log = LoggerFactory.getLogger(EntityMetaBuilder.class);

	private PropertyMeta<?, ?> idMeta;
	private String className;
	private String columnFamilyName;
	private Long serialVersionUID;
	private Map<String, PropertyMeta<?, ?>> propertyMetas;
	private boolean wideRow = false;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

	public static EntityMetaBuilder entityMetaBuilder(PropertyMeta<?, ?> idMeta)
	{
		return new EntityMetaBuilder(idMeta);
	}

	public EntityMetaBuilder(PropertyMeta<?, ?> idMeta) {
		this.idMeta = idMeta;
	}

	public EntityMeta build()
	{
		log.debug("Build entityMeta for entity class {}", className);

		Validator.validateNotNull(idMeta, "idMeta should not be null");
		Validator.validateNotNull(serialVersionUID, "serialVersionUID should not be null");
		Validator.validateNotEmpty(propertyMetas, "propertyMetas map should not be empty");
		Validator.validateRegExp(columnFamilyName, EntityMeta.COLUMN_FAMILY_PATTERN,
				"columnFamilyName");

		EntityMeta meta = new EntityMeta();

		meta.setIdMeta(idMeta);
		meta.setIdClass(idMeta.getValueClass());
		meta.setClassName(className);
		meta.setTableName(columnFamilyName);
		meta.setSerialVersionUID(serialVersionUID);
		meta.setPropertyMetas(Collections.unmodifiableMap(propertyMetas));
		meta.setGetterMetas(Collections.unmodifiableMap(this.extractGetterMetas(propertyMetas)));
		meta.setSetterMetas(Collections.unmodifiableMap(this.extractSetterMetas(propertyMetas)));
		meta.setWideRow(wideRow);
		meta.setConsistencyLevels(consistencyLevels);

		return meta;
	}

	private Map<Method, PropertyMeta<?, ?>> extractGetterMetas(
			Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		Map<Method, PropertyMeta<?, ?>> getterMetas = new HashMap<Method, PropertyMeta<?, ?>>();
		for (PropertyMeta<?, ?> propertyMeta : propertyMetas.values())
		{
			getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		}
		return getterMetas;
	}

	private Map<Method, PropertyMeta<?, ?>> extractSetterMetas(
			Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		Map<Method, PropertyMeta<?, ?>> setterMetas = new HashMap<Method, PropertyMeta<?, ?>>();
		for (PropertyMeta<?, ?> propertyMeta : propertyMetas.values())
		{
			setterMetas.put(propertyMeta.getSetter(), propertyMeta);
		}
		return setterMetas;
	}

	public EntityMetaBuilder className(String className)
	{
		this.className = className;
		return this;
	}

	public EntityMetaBuilder columnFamilyName(String columnFamilyName)
	{
		this.columnFamilyName = columnFamilyName;
		return this;
	}

	public EntityMetaBuilder serialVersionUID(long serialVersionUID)
	{
		this.serialVersionUID = serialVersionUID;
		return this;
	}

	public EntityMetaBuilder propertyMetas(Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		this.propertyMetas = propertyMetas;
		return this;
	}

	public EntityMetaBuilder wideRow(boolean wideRow)
	{
		this.wideRow = wideRow;
		return this;
	}

	public EntityMetaBuilder consistencyLevels(
			Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		this.consistencyLevels = consistencyLevels;
		return this;
	}
}
