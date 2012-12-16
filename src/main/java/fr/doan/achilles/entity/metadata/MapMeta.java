package fr.doan.achilles.entity.metadata;


public class MapMeta<K, V> extends PropertyMeta<K, V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.MAP;
	}
}
