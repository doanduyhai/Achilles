package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IteratorWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorWrapper<ID, V> extends AbstractWrapper<ID, Void, V> implements Iterator<V>
{
	private static final Logger log = LoggerFactory.getLogger(IteratorWrapper.class);

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
			log.trace(
					"Build proxy for join entity for property {} of entity class {} upon next() call",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
			AchillesPersistenceContext<?> joinContext = context.newPersistenceContext(
					propertyMeta.joinMeta(), value);
			return proxifier.buildProxy(value, joinContext);
		}
		else
		{
			return value;
		}
	}

	@Override
	public void remove()
	{
		log.trace("Mark property {} of entity class {} as dirty upon element removal",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		this.target.remove();
		this.markDirty();
	}
}
