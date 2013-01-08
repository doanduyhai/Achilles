package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.Map;

import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.dao.GenericDynamicCompositeDao;

/**
 * EntityMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMeta<ID>
{

	public static final String COLUMN_FAMILY_PATTERN = "[a-zA-Z0-9_]+";
	private String className;
	private String columnFamilyName;
	private Long serialVersionUID;
	private Serializer<?> idSerializer;
	private Map<String, PropertyMeta<?, ?>> propertyMetas;
	private PropertyMeta<Void, ID> idMeta;
	private GenericDynamicCompositeDao<ID> entityDao;
	private GenericCompositeDao<ID, ?> wideRowDao;
	private Map<Method, PropertyMeta<?, ?>> getterMetas;
	private Map<Method, PropertyMeta<?, ?>> setterMetas;
	private boolean wideRow = false;

	public String getClassName()
	{
		return className;
	}

	public void setClassName(String className)
	{
		this.className = className;
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

	public Map<String, PropertyMeta<?, ?>> getPropertyMetas()
	{
		return propertyMetas;
	}

	public void setPropertyMetas(Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		this.propertyMetas = propertyMetas;
	}

	public PropertyMeta<Void, ID> getIdMeta()
	{
		return idMeta;
	}

	public void setIdMeta(PropertyMeta<Void, ID> idMeta)
	{
		this.idMeta = idMeta;
	}

	public GenericDynamicCompositeDao<ID> getEntityDao()
	{
		return entityDao;
	}

	public void setEntityDao(GenericDynamicCompositeDao<ID> entityDao)
	{
		this.entityDao = entityDao;
	}

	public Map<Method, PropertyMeta<?, ?>> getGetterMetas()
	{
		return getterMetas;
	}

	public void setGetterMetas(Map<Method, PropertyMeta<?, ?>> getterMetas)
	{
		this.getterMetas = getterMetas;
	}

	public Map<Method, PropertyMeta<?, ?>> getSetterMetas()
	{
		return setterMetas;
	}

	public void setSetterMetas(Map<Method, PropertyMeta<?, ?>> setterMetas)
	{
		this.setterMetas = setterMetas;
	}

	public boolean isWideRow()
	{
		return wideRow;
	}

	public void setWideRow(boolean wideRow)
	{
		this.wideRow = wideRow;
	}

	public GenericCompositeDao<ID, ?> getWideRowDao()
	{
		return wideRowDao;
	}

	public void setWideRowDao(GenericCompositeDao<ID, ?> wideRowDao)
	{
		this.wideRowDao = wideRowDao;
	}
}
