package fr.doan.achilles.entity.metadata;


public class ListLazyPropertyMeta<V> extends ListPropertyMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_LIST;
	}
}
