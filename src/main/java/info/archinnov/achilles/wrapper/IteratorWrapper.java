package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.manager.PersistenceContext;

import java.util.Iterator;

/**
 * IteratorWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorWrapper<ID, V> extends AbstractWrapper<ID, Void, V> implements Iterator<V>
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
		V value = this.target.next();
		if (isJoin())
		{
			PersistenceContext<?> joinContext = context.newPersistenceContext(
					propertyMeta.joinMeta(), value);
			return helper.buildProxy(value, joinContext);
		}
		else
		{

			return value;
		}
	}

	@Override
	public void remove()
	{
		this.target.remove();
		this.markDirty();
	}
}
