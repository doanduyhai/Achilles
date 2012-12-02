package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.Serializer;

/**
 * InternalMultiKeyWideMapPropertyMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWideMapMeta<K, V> extends
		WideMapMeta<K, V>
{
	private List<Serializer<?>> keySerializers;
	private List<Method> keyGetters;

	@Override
	public boolean isSingleKey()
	{
		return false;
	}

	public List<Serializer<?>> getKeySerializers()
	{
		return keySerializers;
	}

	public void setKeySerializers(List<Serializer<?>> keySerializers)
	{
		this.keySerializers = keySerializers;
	}

	public List<Method> getKeyGetters()
	{
		return keyGetters;
	}

	public void setKeyGetters(List<Method> keyGetters)
	{
		this.keyGetters = keyGetters;
	}

}
