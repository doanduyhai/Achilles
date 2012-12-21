package fr.doan.achilles.entity.metadata.builder;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.dao.GenericMultiKeyWideRowDao;
import fr.doan.achilles.dao.GenericWideRowDao;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.validation.Validator;

public class EntityMetaBuilder<ID>
{

	private PropertyMeta<Void, ID> idMeta;
	private String canonicalClassName;
	private String columnFamilyName;
	private Long serialVersionUID;
	private Map<String, PropertyMeta<?, ?>> propertyMetas;
	private Keyspace keyspace;
	private boolean wideRow = false;

	public static <ID> EntityMetaBuilder<ID> entityMetaBuilder(PropertyMeta<Void, ID> idMeta)
	{
		return new EntityMetaBuilder<ID>(idMeta);
	}

	public EntityMetaBuilder(PropertyMeta<Void, ID> idMeta) {
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
		Validator.validateRegExp(columnFamilyName, EntityMeta.COLUMN_FAMILY_PATTERN,
				"columnFamilyName");

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
		meta.setWideRow(wideRow);

		if (wideRow)
		{
			PropertyMeta<?, ?> propertyMeta = propertyMetas.entrySet().iterator().next().getValue();
			if (propertyMeta.isSingleKey())
			{
				meta.setWideRowDao(new GenericWideRowDao(keyspace, idSerializer, propertyMeta
						.getKeySerializer(), this.columnFamilyName));
			}
			else
			{

				meta.setWideRowMultiKeyDao(new GenericMultiKeyWideRowDao(keyspace, idSerializer,
						this.columnFamilyName));
			}
		}
		else
		{
			meta.setEntityDao(new GenericEntityDao(keyspace, idSerializer, this.columnFamilyName));
		}
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

	public EntityMetaBuilder<ID> propertyMetas(Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		this.propertyMetas = propertyMetas;
		return this;
	}

	public EntityMetaBuilder<ID> keyspace(Keyspace keyspace)
	{
		this.keyspace = keyspace;
		return this;
	}

	public EntityMetaBuilder<ID> wideRow(boolean wideRow)
	{
		this.wideRow = wideRow;
		return this;
	}
}