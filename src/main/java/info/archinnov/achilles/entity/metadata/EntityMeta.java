package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

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
	private Class<ID> idClass;
	private Map<String, PropertyMeta<?, ?>> propertyMetas;
	private PropertyMeta<Void, ID> idMeta;
	private Map<Method, PropertyMeta<?, ?>> getterMetas;
	private Map<Method, PropertyMeta<?, ?>> setterMetas;
	private boolean wideRow = false;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

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

	public Pair<ConsistencyLevel, ConsistencyLevel> getConsistencyLevels()
	{
		return this.consistencyLevels;
	}

	public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		this.consistencyLevels = consistencyLevels;
	}

	public Class<ID> getIdClass()
	{
		return idClass;
	}

	public void setIdClass(Class<ID> idClass)
	{
		this.idClass = idClass;
	}

	@Override
	public String toString()
	{
		StringBuilder description = new StringBuilder();
		description.append("EntityMeta [className=").append(className).append(", ");
		description.append("columnFamilyName=").append(columnFamilyName).append(", ");
		description.append("serialVersionUID=").append(serialVersionUID).append(", ");
		description.append("idSerializer=").append(LONG_SRZ.getComparatorType().getTypeName())
				.append(", ");
		description.append("propertyMetas=[").append(StringUtils.join(propertyMetas.keySet(), ","))
				.append("], ");
		description.append("idMeta=").append(idMeta.toString()).append(", ");
		description.append("wideRow=").append(wideRow).append(", ");
		description.append("consistencyLevels=[").append(consistencyLevels.left.name()).append(",")
				.append(consistencyLevels.right.name()).append("]]");
		return description.toString();
	}
}
