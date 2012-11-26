package fr.doan.achilles.entity.metadata;


public class SetLazyPropertyMeta<V> extends SetPropertyMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_SET;
	}
}
