package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.Serializer;

/**
 * InternalMultiKeyWideMapPropertyMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWideMapMeta<K, V> extends WideMapMeta<K, V>
{
	private List<Serializer<?>> componentSerializers;
	private List<Method> componentGetters;

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.WIDE_MAP;
	}

	@Override
	public boolean isSingleKey()
	{
		return false;
	}

	@Override
	public boolean isLazy()
	{
		return true;
	}

	public boolean isInternal()
	{
		return true;
	}

	public List<Serializer<?>> getComponentSerializers()
	{
		return componentSerializers;
	}

	public void setComponentSerializers(List<Serializer<?>> componentSerializers)
	{
		this.componentSerializers = componentSerializers;
	}

	public List<Method> getComponentGetters()
	{
		return componentGetters;
	}

	public void setComponentGetters(List<Method> componentGetters)
	{
		this.componentGetters = componentGetters;
	}
}
