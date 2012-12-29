package fr.doan.achilles.entity.metadata;

import javax.persistence.CascadeType;

/**
 * JoinMetaData
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinProperties<K>
{

	private EntityMeta<K> entityMeta;
	private CascadeType cascadeType;
	// private boolean insertable = false;
	// private boolean updatable = false;
	private boolean nullable = true;

	public JoinProperties() {}

	public JoinProperties(EntityMeta<K> entityMeta) {
		this.entityMeta = entityMeta;
	}

	@SuppressWarnings("rawtypes")
	public EntityMeta getEntityMeta()
	{
		return entityMeta;
	}

	public void setEntityMeta(EntityMeta<K> entityMeta)
	{
		this.entityMeta = entityMeta;
	}

	public CascadeType getCascadeType()
	{
		return cascadeType;
	}

	public void setCascadeType(CascadeType cascadeType)
	{
		this.cascadeType = cascadeType;
	}

	// public boolean isInsertable()
	// {
	// return insertable;
	// }
	//
	// public void setInsertable(boolean insertable)
	// {
	// this.insertable = insertable;
	// }
	//
	// public boolean isUpdatable()
	// {
	// return updatable;
	// }
	//
	// public void setUpdatable(boolean updatable)
	// {
	// this.updatable = updatable;
	// }

	public boolean isNullable()
	{
		return nullable;
	}

	public void setNullable(boolean nullable)
	{
		this.nullable = nullable;
	}
}
