package fr.doan.achilles.entity.metadata;

/**
 * WideMapPropertyMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapMeta<K, V> extends MapMeta<K, V>
{

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.WIDE_MAP;
	}

	public boolean isSingleKey()
	{
		return true;
	}

	@Override
	public boolean isLazy()
	{
		return true;
	}

	public boolean isInternal()
	{
		return true;
	}
}
