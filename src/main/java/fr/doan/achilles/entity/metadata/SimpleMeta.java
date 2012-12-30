package fr.doan.achilles.entity.metadata;

public class SimpleMeta<V> extends PropertyMeta<Void, V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.SIMPLE;
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
