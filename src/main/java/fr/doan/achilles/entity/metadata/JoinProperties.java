package fr.doan.achilles.entity.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.CascadeType;

import fr.doan.achilles.exception.BeanMappingException;

/**
 * JoinMetaData
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings("rawtypes")
public class JoinProperties
{

	private EntityMeta entityMeta;
	private List<CascadeType> cascadeTypes = new ArrayList<CascadeType>();

	public EntityMeta getEntityMeta()
	{
		return entityMeta;
	}

	public void setEntityMeta(EntityMeta entityMeta)
	{
		this.entityMeta = entityMeta;
	}

	public List<CascadeType> getCascadeTypes()
	{
		return cascadeTypes;
	}

	public void setCascadeTypes(List<CascadeType> cascadeTypes)
	{
		this.cascadeTypes = cascadeTypes;
	}

	public void addCascadeType(CascadeType cascadeType)
	{
		this.cascadeTypes.add(cascadeType);
	}

	public void addCascadeType(Collection<CascadeType> cascadeTypesCollection)
	{
		if (cascadeTypesCollection.contains(CascadeType.REMOVE))
		{
			throw new BeanMappingException("CascadeType.REMOVE is not supported for join columns");
		}
		this.cascadeTypes.addAll(cascadeTypesCollection);
	}
}
