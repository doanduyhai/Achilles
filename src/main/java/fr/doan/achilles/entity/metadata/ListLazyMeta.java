package fr.doan.achilles.entity.metadata;

public class ListLazyMeta<V> extends ListMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_LIST;
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
