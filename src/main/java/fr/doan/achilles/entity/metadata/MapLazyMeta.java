package fr.doan.achilles.entity.metadata;

public class MapLazyMeta<K, V> extends MapMeta<K, V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_MAP;
	}

	@Override
	public boolean isSingleKey()
	{
		return true;
	}

	@Override
	public boolean isLazy()
	{
		return true;
	}

	@Override
	public boolean isJoinColumn()
	{
		return false;
	}
}
