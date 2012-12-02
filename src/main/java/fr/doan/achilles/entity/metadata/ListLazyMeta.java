package fr.doan.achilles.entity.metadata;


public class ListLazyMeta<V> extends ListMeta<V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_LIST;
	}
}
