package fr.doan.achilles.wrapper;

import java.lang.reflect.Method;
import java.util.Map;

import fr.doan.achilles.entity.metadata.PropertyMeta;

public abstract class AbstractProxy<E>
{
	protected Map<Method, PropertyMeta<?>> dirtyMap;
	protected Method setter;
	protected PropertyMeta<E> propertyMeta;

	public Map<Method, PropertyMeta<?>> getDirtyMap()
	{
		return dirtyMap;
	}

	public void setDirtyMap(Map<Method, PropertyMeta<?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
	}

	public void setSetter(Method setter)
	{
		this.setter = setter;
	}

	public void setPropertyMeta(PropertyMeta<E> propertyMeta)
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
}
