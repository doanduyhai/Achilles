package fr.doan.achilles.entity.metadata;

/**
 * JoinMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinMeta<V> extends PropertyMeta<Void, V>
{

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.JOIN_SIMPLE;
	}

	@Override
	public boolean isSingleKey()
	{
		return true;
	}

	@Override
	public boolean isLazy()
	{
		return true;
	}

	@Override
	public boolean isJoinColumn()
	{
		return true;
	}

}
