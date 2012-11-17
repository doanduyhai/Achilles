package fr.doan.achilles.metadata;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.validation.Validator;

public class EntityMeta<ID extends Serializable>
{
	private String canonicalClassName;
	private String columnFamilyName;
	private Long serialVersionUID;
	private Class<ID> idClass;
	private Serializer<?> idSerializer;
	private Map<String, PropertyMeta<?>> attributes;

	public EntityMeta(Class<ID> idClass, String canonicalClassName, Long serialVersionUID, Map<String, PropertyMeta<?>> propertieMetas) {
		this(idClass, canonicalClassName, normalizeColumnFamilyName(canonicalClassName), serialVersionUID, propertieMetas);
	}

	public EntityMeta(Class<ID> idClass, String canonicalClassName, String columnFamilyName, Long serialVersionUID,
			Map<String, PropertyMeta<?>> propertieMetas)
	{
		super();

		Validator.validateNotNull(idClass, "idClass");
		Validator.validateNotBlank(canonicalClassName, "canonicalClassName");
		Validator.validateNotBlank(columnFamilyName, "columnFamilyName");
		Validator.validateNotNull(serialVersionUID, "serialVersionUID");
		Validator.validateNotEmpty(propertieMetas, "propertieMetas");

		this.idClass = idClass;
		this.idSerializer = SerializerTypeInferer.getSerializer(idClass);
		this.canonicalClassName = canonicalClassName;
		this.columnFamilyName = normalizeColumnFamilyName(columnFamilyName);
		this.serialVersionUID = serialVersionUID;
		this.attributes = Collections.unmodifiableMap(propertieMetas);
	}

	public static String normalizeColumnFamilyName(String columnFamilyName)
	{
		return columnFamilyName.replaceAll("\\.", "_").replaceAll("\\$", "_I_");
	}

	public String getCanonicalClassName()
	{
		return canonicalClassName;
	}

	public String getColumnFamilyName()
	{
		return columnFamilyName;
	}

	public Long getSerialVersionUID()
	{
		return serialVersionUID;
	}

	public Map<String, PropertyMeta<?>> getAttributes()
	{
		return attributes;
	}

	public Class<ID> getIdClass()
	{
		return idClass;
	}

	public Serializer<?> getIdSerializer()
	{
		return idSerializer;
	}

}
