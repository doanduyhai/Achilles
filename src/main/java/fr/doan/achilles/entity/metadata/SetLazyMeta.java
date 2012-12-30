package fr.doan.achilles.entity.metadata;

public class SetLazyMeta<V> extends SetMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_SET;
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
