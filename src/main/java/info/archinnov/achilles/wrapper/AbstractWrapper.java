package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * AbstractWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AbstractWrapper<K, V>
{
	protected Map<Method, PropertyMeta<?, ?>> dirtyMap;
	protected Method setter;
	protected PropertyMeta<K, V> propertyMeta;
	protected EntityHelper helper;

	public Map<Method, PropertyMeta<?, ?>> getDirtyMap()
	{
		return dirtyMap;
	}

	public void setDirtyMap(Map<Method, PropertyMeta<?, ?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
	}

	public void setSetter(Method setter)
	{
		this.setter = setter;
	}

	public void setPropertyMeta(PropertyMeta<K, V> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
	}

	protected void markDirty()
	{
		if (!dirtyMap.containsKey(this.setter))
		{
			dirtyMap.put(this.setter, this.propertyMeta);
		}
	}

	public void setHelper(EntityHelper helper)
	{
		this.helper = helper;
	}

	protected boolean isJoin()
	{
		return this.propertyMeta.type().isJoinColumn();
	}

	protected EntityMeta<?> joinMeta()
	{
		if (isJoin())
		{
			return this.propertyMeta.getJoinProperties().getEntityMeta();
		}
		else
		{
			return null;
		}
	}
}
