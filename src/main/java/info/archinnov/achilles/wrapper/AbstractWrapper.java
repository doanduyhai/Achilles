package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.AchillesEntityProxifier;

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
	protected AchillesEntityProxifier proxifier;
	protected AchillesPersistenceContext context;

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
		if (!dirtyMap.containsKey(setter))
		{
			dirtyMap.put(setter, propertyMeta);
		}
	}

	public void setProxifier(AchillesEntityProxifier proxifier)
	{
		this.proxifier = proxifier;
	}

	protected boolean isJoin()
	{
		return this.propertyMeta.type().isJoinColumn();
	}

	public void setContext(AchillesPersistenceContext context)
	{
		this.context = context;
	}

	protected AchillesPersistenceContext joinContext(Object joinEntity)
	{
		return context.newPersistenceContext(propertyMeta.joinMeta(), joinEntity);
	}
}
