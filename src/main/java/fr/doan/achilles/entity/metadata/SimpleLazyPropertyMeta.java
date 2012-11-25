package fr.doan.achilles.entity.metadata;

import java.io.Serializable;

public class SimpleLazyPropertyMeta<V extends Serializable> extends SimplePropertyMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_SIMPLE;
	}
}
