package fr.doan.achilles.entity.metadata;

public class SimpleMeta<V> extends PropertyMeta<Void, V>
{
	@Override
	public PropertyType propertyType()
	{
		return PropertyType.SIMPLE;
	}
}
