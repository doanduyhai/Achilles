package fr.doan.achilles.entity.metadata;

import java.util.HashSet;
import java.util.Set;

public class SetPropertyMeta<V> extends SimplePropertyMeta<V>
{
	public Set<V> newSetInstance()
	{
		return new HashSet<V>();
	}

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.SET;
	}

}
