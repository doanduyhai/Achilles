package fr.doan.achilles.entity.metadata;


public class SimpleLazyMeta<V> extends SimpleMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_SIMPLE;
	}
}
