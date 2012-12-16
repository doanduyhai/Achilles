package fr.doan.achilles.entity.metadata;

public class SetLazyMeta<V> extends SetMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_SET;
	}
}
