package fr.doan.achilles.entity.metadata;

/**
 * MultiKeyJoinWideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyJoinWideMapMeta<K, V> extends PropertyMeta<K, V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.JOIN_WIDE_MAP;
	}

	@Override
	public boolean isSingleKey()
	{
		return false;
	}

	@Override
	public boolean isLazy()
	{
		return true;
	}

	@Override
	public boolean isJoinColumn()
	{
		return true;
	}
}
