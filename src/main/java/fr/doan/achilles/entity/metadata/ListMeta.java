package fr.doan.achilles.entity.metadata;

import java.util.ArrayList;
import java.util.List;

public class ListMeta<V> extends SimpleMeta<V>
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
