package fr.doan.achilles.entity.metadata;

import java.util.HashSet;
import java.util.Set;

public class SetMeta<V> extends PropertyMeta<Void, V>
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
