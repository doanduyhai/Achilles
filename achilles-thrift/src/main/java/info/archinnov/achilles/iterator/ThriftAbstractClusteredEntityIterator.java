package info.archinnov.achilles.iterator;

import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Sets;

/**
 * ThriftAbstractClusteredEntityIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class ThriftAbstractClusteredEntityIterator<T> implements Iterator<T> {

    private static final Logger log = LoggerFactory.getLogger(ThriftAbstractClusteredEntityIterator.class);

    protected Class<T> entityClass;
    private Iterator<?> iterator;
    protected ThriftPersistenceContext context;

    private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
    protected ThriftCompositeTransformer transformer = new ThriftCompositeTransformer();

    public ThriftAbstractClusteredEntityIterator(Class<T> entityClass, Iterator<?> iterator,
            ThriftPersistenceContext context) {
        this.entityClass = entityClass;
        this.iterator = iterator;
        this.context = context;
    }

    @Override
    public boolean hasNext() {
        log.trace("Does the iterator {} has next value ? {} ", iterator, iterator.hasNext());
        return iterator.hasNext();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(
                "Remove from iterator is not supported. Please use entityManager.remove() instead");
    }

    protected T proxifyClusteredEntity(T target)
    {
        return proxifier.buildProxy(target, context.duplicate(target),
                Sets.newHashSet(context.getFirstMeta().getGetter()));
    }
}
