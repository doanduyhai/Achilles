package fr.doan.achilles.entity.metadata;

public class MapLazyPropertyMeta<K, V> extends MapPropertyMeta<K, V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LAZY_MAP;
	}
}
