package fr.doan.achilles.entity.metadata;


/**
 * InternalMultiKeyWideMapPropertyMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWideMapMeta<K, V> extends PropertyMeta<K, V>
{

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.WIDE_MAP;
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

	public boolean isJoinColumn()
	{
		return true;
	}
}
