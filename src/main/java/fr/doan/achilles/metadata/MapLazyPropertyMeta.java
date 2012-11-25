package fr.doan.achilles.metadata;

import java.io.Serializable;

public class MapLazyPropertyMeta<V extends Serializable> extends MapPropertyMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_MAP;
	}
}
