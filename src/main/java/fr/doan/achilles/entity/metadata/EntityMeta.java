package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.Map;

import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.dao.GenericDao;

public class EntityMeta<ID>
{

	public static final String COLUMN_FAMILY_PATTERN = "[a-zA-Z0-9_]+";
	private String canonicalClassName;
	private String columnFamilyName;
	private Long serialVersionUID;
	private Serializer<?> idSerializer;
	private Map<String, PropertyMeta<?>> propertyMetas;
	private PropertyMeta<ID> idMeta;
	private GenericDao<ID> dao;
	private Map<Method, PropertyMeta<?>> getterMetas;
	private Map<Method, PropertyMeta<?>> setterMetas;

	public String getCanonicalClassName()
	{
		return canonicalClassName;
	}

	public void setCanonicalClassName(String canonicalClassName)
	{
		this.canonicalClassName = canonicalClassName;
	}

	public String getColumnFamilyName()
	{
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName)
	{
		this.columnFamilyName = columnFamilyName;
	}

	public Long getSerialVersionUID()
	{
		return serialVersionUID;
	}

	public void setSerialVersionUID(Long serialVersionUID)
	{
		this.serialVersionUID = serialVersionUID;
	}

	public Serializer<?> getIdSerializer()
	{
		return idSerializer;
	}

	public void setIdSerializer(Serializer<?> idSerializer)
	{
		this.idSerializer = idSerializer;
	}

	public Map<String, PropertyMeta<?>> getPropertyMetas()
	{
		return propertyMetas;
	}

	public void setPropertyMetas(Map<String, PropertyMeta<?>> propertyMetas)
	{
		this.propertyMetas = propertyMetas;
	}

	public PropertyMeta<ID> getIdMeta()
	{
		return idMeta;
	}

	public void setIdMeta(PropertyMeta<ID> idMeta)
	{
		this.idMeta = idMeta;
	}

	public GenericDao<ID> getDao()
	{
		return dao;
	}

	public void setDao(GenericDao<ID> dao)
	{
		this.dao = dao;
	}

	public Map<Method, PropertyMeta<?>> getGetterMetas()
	{
		return getterMetas;
	}

	public void setGetterMetas(Map<Method, PropertyMeta<?>> getterMetas)
	{
		this.getterMetas = getterMetas;
	}

	public Map<Method, PropertyMeta<?>> getSetterMetas()
	{
		return setterMetas;
	}

	public void setSetterMetas(Map<Method, PropertyMeta<?>> setterMetas)
	{
		this.setterMetas = setterMetas;
	}

}
