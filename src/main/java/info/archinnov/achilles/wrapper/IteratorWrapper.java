package info.archinnov.achilles.wrapper;

import java.util.Iterator;

/**
 * IteratorWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorWrapper<V> extends AbstractWrapper<Void, V> implements Iterator<V>
{
	protected Iterator<V> target;

	public IteratorWrapper(Iterator<V> target) {
		this.target = target;
	}

	@Override
	public boolean hasNext()
	{
		return this.target.hasNext();
	}

	@Override
	public V next()
	{
		if (isJoin())
		{
			return helper.buildProxy(this.target.next(), joinMeta());
		}
		else
		{

			return this.target.next();
		}
	}

	@Override
	public void remove()
	{
		this.target.remove();
		this.markDirty();
	}
}
