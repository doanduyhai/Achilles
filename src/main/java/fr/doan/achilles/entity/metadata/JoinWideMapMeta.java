package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;

/**
 * JoinWideMapMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapMeta<K, V> extends MapMeta<K, V>
{
	private String joinColumnFamily;
	private String joinKey;
	private Method joinKeyGetter;

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.JOIN_WIDE_MAP;
	}

	public boolean isSingleKey()
	{
		return true;
	}

	@Override
	public boolean isLazy()
	{
		return true;
	}

	public boolean isJoinColumn()
	{
		return true;
	}
}
