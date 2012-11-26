package fr.doan.achilles.entity.metadata;


public class SimpleLazyPropertyMeta<V> extends SimplePropertyMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_SIMPLE;
	}
}
