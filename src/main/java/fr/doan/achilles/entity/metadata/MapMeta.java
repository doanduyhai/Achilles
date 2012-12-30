package fr.doan.achilles.entity.metadata;

import java.util.HashMap;
import java.util.Map;

public class MapMeta<K, V> extends PropertyMeta<K, V>
{
	public Map<K, V> newMapInstance()
	{
		return new HashMap<K, V>();
	}

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.MAP;
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
