package fr.doan.achilles.entity.metadata;

import java.util.ArrayList;
import java.util.List;

public class ListPropertyMeta<V> extends SimplePropertyMeta<V>
{

	public List<V> newListInstance()
	{
		return new ArrayList<V>();
	}

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LIST;
	}

}
