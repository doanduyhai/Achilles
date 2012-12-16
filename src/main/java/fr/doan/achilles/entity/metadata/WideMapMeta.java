package fr.doan.achilles.entity.metadata;

/**
 * WideMapPropertyMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapMeta<K, V> extends PropertyMeta<K, V>
{

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.WIDE_MAP;
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
