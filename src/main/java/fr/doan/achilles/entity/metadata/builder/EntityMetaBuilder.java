package fr.doan.achilles.entity.metadata.builder;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.validation.Validator;

public class EntityMetaBuilder<ID extends Serializable>
{

	private PropertyMeta<ID> idMeta;
	private String canonicalClassName;
	private String columnFamilyName;
	private Long serialVersionUID;
	private GenericDao<?> dao;
	private Map<String, PropertyMeta<?>> propertyMetas;
	private Keyspace keyspace;

	public static <ID extends Serializable> EntityMetaBuilder<ID> entityMetaBuilder(PropertyMeta<ID> idMeta)
	{
		return new EntityMetaBuilder<ID>(idMeta);
	}

	public EntityMetaBuilder(PropertyMeta<ID> idMeta) {
		this.idMeta = idMeta;
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public EntityMeta build()
	{

		Validator.validateNotNull(keyspace, "keyspace");
		Validator.validateNotNull(idMeta, "idMeta");
		Validator.validateNotBlank(canonicalClassName, "canonicalClassName");
		if (!StringUtils.isBlank(columnFamilyName))
		{
			columnFamilyName = normalizeColumnFamilyName(columnFamilyName);
		}
		else
		{
			columnFamilyName = normalizeColumnFamilyName(canonicalClassName);
		}
		Validator.validateNotNull(serialVersionUID, "serialVersionUID");
		Validator.validateNotEmpty(propertyMetas, "propertyMetas");
		Validator.validateRegExp(columnFamilyName, EntityMeta.COLUMN_FAMILY_PATTERN, "columnFamilyName");

		EntityMeta meta = new EntityMeta();

		meta.setIdMeta(idMeta);
		Serializer<?> idSerializer = SerializerTypeInferer.getSerializer(idMeta.getValueClass());
		meta.setIdSerializer(idSerializer);
		meta.setCanonicalClassName(canonicalClassName);
		meta.setColumnFamilyName(columnFamilyName);
		meta.setSerialVersionUID(serialVersionUID);
		meta.setPropertyMetas(Collections.unmodifiableMap(propertyMetas));
		meta.setGetterMetas(Collections.unmodifiableMap(this.extractGetterMetas(propertyMetas)));
		meta.setSetterMetas(Collections.unmodifiableMap(this.extractSetterMetas(propertyMetas)));

		this.dao = new GenericDao(keyspace, idSerializer, this.columnFamilyName);

		meta.setDao(dao);

		return meta;
	}

	private Map<Method, PropertyMeta<?>> extractGetterMetas(Map<String, PropertyMeta<?>> propertyMetas)
	{
		Map<Method, PropertyMeta<?>> getterMetas = new HashMap<Method, PropertyMeta<?>>();
		for (PropertyMeta<?> propertyMeta : propertyMetas.values())
		{
			getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		}
		return getterMetas;
	}

	private Map<Method, PropertyMeta<?>> extractSetterMetas(Map<String, PropertyMeta<?>> propertyMetas)
	{
		Map<Method, PropertyMeta<?>> setterMetas = new HashMap<Method, PropertyMeta<?>>();
		for (PropertyMeta<?> propertyMeta : propertyMetas.values())
		{
			setterMetas.put(propertyMeta.getSetter(), propertyMeta);
		}
		return setterMetas;
	}

	public static String normalizeColumnFamilyName(String columnFamilyName)
	{
		return columnFamilyName.replaceAll("\\.", "_").replaceAll("\\$", "_I_");
	}

	public EntityMetaBuilder<ID> canonicalClassName(String canonicalClassName)
	{
		this.canonicalClassName = canonicalClassName;
		return this;
	}

	public EntityMetaBuilder<ID> columnFamilyName(String columnFamilyName)
	{
		this.columnFamilyName = columnFamilyName;
		return this;
	}

	public EntityMetaBuilder<ID> serialVersionUID(long serialVersionUID)
	{
		this.serialVersionUID = serialVersionUID;
		return this;
	}

	public EntityMetaBuilder<ID> propertyMetas(Map<String, PropertyMeta<?>> propertyMetas)
	{
		this.propertyMetas = propertyMetas;
		return this;
	}

	public EntityMetaBuilder<ID> keyspace(Keyspace keyspace)
	{
		this.keyspace = keyspace;
		return this;
	}
}
