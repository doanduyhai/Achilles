package fr.doan.achilles.entity.metadata;

import java.util.ArrayList;
import java.util.List;

public class ListMeta<V> extends PropertyMeta<Void, V>
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

	@Override
	public boolean isSingleKey()
	{
		return true;
	}

	@Override
	public boolean isLazy()
	{
		return false;
	}

	@Override
	public boolean isJoinColumn()
	{
		return false;
	}
}
