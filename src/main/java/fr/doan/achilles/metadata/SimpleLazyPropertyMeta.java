package fr.doan.achilles.metadata;

import java.io.Serializable;

public class SimpleLazyPropertyMeta<V extends Serializable> extends SimplePropertyMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_SIMPLE;
	}
}
