package fr.doan.achilles.metadata;

import java.io.Serializable;

public class SetLazyPropertyMeta<V extends Serializable> extends SetPropertyMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_SET;
	}
}
