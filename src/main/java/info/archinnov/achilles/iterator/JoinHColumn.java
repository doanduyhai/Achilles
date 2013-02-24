package info.archinnov.achilles.iterator;

import java.nio.ByteBuffer;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * JoinHColumn
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinHColumn<N, V> implements HColumn<N, V>
{

	private N name;
	private V value;
	private int ttl;

	@Override
	public HColumn<N, V> setName(N name)
	{
		this.name = name;
		return this;
	}

	@Override
	public HColumn<N, V> setValue(V value)
	{
		this.value = value;
		return this;
	}

	@Override
	public N getName()
	{
		return this.name;
	}

	@Override
	public V getValue()
	{
		return this.value;
	}

	@Override
	public ByteBuffer getValueBytes()
	{
		return null;
	}

	@Override
	public ByteBuffer getNameBytes()
	{
		return null;
	}

	@Override
	public long getClock()
	{
		return 0;
	}

	@Override
	public HColumn<N, V> setClock(long clock)
	{
		return this;
	}

	@Override
	public int getTtl()
	{
		return ttl;
	}

	@Override
	public HColumn<N, V> setTtl(int ttl)
	{
		this.ttl = ttl;
		return this;
	}

	@Override
	public HColumn<N, V> clear()
	{
		this.name = null;
		this.value = null;
		this.ttl = 0;
		return this;
	}

	@Override
	public HColumn<N, V> apply(V value, long clock, int ttl)
	{
		this.value = value;
		this.ttl = ttl;
		return this;
	}

	@Override
	public Serializer<N> getNameSerializer()
	{
		return null;
	}

	@Override
	public Serializer<V> getValueSerializer()
	{
		return null;
	}

}
