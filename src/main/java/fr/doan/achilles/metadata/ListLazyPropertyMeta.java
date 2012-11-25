package fr.doan.achilles.metadata;

import java.io.Serializable;

public class ListLazyPropertyMeta<V extends Serializable> extends ListPropertyMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_LIST;
	}
}
