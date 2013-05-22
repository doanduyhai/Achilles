package info.archinnov.achilles.proxy.wrapper;

import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AchillesListIteratorWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesListIteratorWrapper<V> extends AchillesAbstractWrapper<Void, V> implements
		ListIterator<V>
{
	private static final Logger log = LoggerFactory.getLogger(AchillesListIteratorWrapper.class);

	private ListIterator<V> target;

	public AchillesListIteratorWrapper(ListIterator<V> target) {
		this.target = target;
	}

	@Override
	public void add(V e)
	{
		log.trace("Mark list property {} of entity class {} dirty upon element addition",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		this.target.add(proxifier.unproxy(e));
		this.markDirty();
	}

	@Override
	public boolean hasNext()
	{
		return this.target.hasNext();
	}

	@Override
	public boolean hasPrevious()
	{
		return this.target.hasPrevious();
	}

	@Override
	public V next()
	{
		log.trace("Return next element from list property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		V entity = this.target.next();
		if (isJoin())
		{
			return proxifier.buildProxy(entity, joinContext(entity));
		}
		else
		{
			return entity;
		}
	}

	@Override
	public int nextIndex()
	{
		return this.target.nextIndex();
	}

	@Override
	public V previous()
	{
		log.trace("Return previous element from list property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		V entity = this.target.previous();
		if (isJoin())
		{
			return proxifier.buildProxy(entity, joinContext(entity));
		}
		else
		{
			return entity;
		}
	}

	@Override
	public int previousIndex()
	{
		return this.target.previousIndex();
	}

	@Override
	public void remove()
	{
		log.trace("Mark list property {} of entity class {} dirty upon element removal",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		this.target.remove();
		this.markDirty();
	}

	@Override
	public void set(V e)
	{
		log
				.trace("Mark list property {} of entity class {} dirty upon element set at current position",
						propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		this.target.set(proxifier.unproxy(e));
		this.markDirty();
	}

}
