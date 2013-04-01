package info.archinnov.achilles.entity.metadata;

import info.archinnov.achilles.exception.AchillesBeanMappingException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;

/**
 * JoinProperties
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinProperties
{

	private EntityMeta<?> entityMeta;
	private Set<CascadeType> cascadeTypes = new HashSet<CascadeType>();

	public EntityMeta<?> getEntityMeta()
	{
		return entityMeta;
	}

	public void setEntityMeta(EntityMeta<?> entityMeta)
	{
		this.entityMeta = entityMeta;
	}

	public Set<CascadeType> getCascadeTypes()
	{
		return cascadeTypes;
	}

	public void setCascadeTypes(Set<CascadeType> cascadeTypes)
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
			throw new AchillesBeanMappingException("CascadeType.REMOVE is not supported for join columns");
		}
		this.cascadeTypes.addAll(cascadeTypesCollection);
	}
}
