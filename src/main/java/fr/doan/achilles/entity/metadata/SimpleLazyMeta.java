package fr.doan.achilles.entity.metadata;

public class SimpleLazyMeta<V> extends PropertyMeta<Void, V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_SIMPLE;
	}
}
