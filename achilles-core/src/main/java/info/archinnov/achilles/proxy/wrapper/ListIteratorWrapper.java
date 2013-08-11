package info.archinnov.achilles.proxy.wrapper;

import java.util.ListIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ListIteratorWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListIteratorWrapper extends AbstractWrapper implements ListIterator<Object> {
    private static final Logger log = LoggerFactory.getLogger(ListIteratorWrapper.class);

    private ListIterator<Object> target;

    public ListIteratorWrapper(ListIterator<Object> target) {
        this.target = target;
    }

    @Override
    public void add(Object e) {
        log.trace("Mark list property {} of entity class {} dirty upon element addition",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        this.target.add(proxifier.unwrap(e));
        this.markDirty();
    }

    @Override
    public boolean hasNext() {
        return this.target.hasNext();
    }

    @Override
    public boolean hasPrevious() {
        return this.target.hasPrevious();
    }

    @Override
    public Object next() {
        log.trace("Return next element from list property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        Object entity = this.target.next();
        if (isJoin()) {
            return proxifier.buildProxy(entity, joinContext(entity));
        } else {
            return entity;
        }
    }

    @Override
    public int nextIndex() {
        return this.target.nextIndex();
    }

    @Override
    public Object previous() {
        log.trace("Return previous element from list property {} of entity class {}", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        Object entity = this.target.previous();
        if (isJoin()) {
            return proxifier.buildProxy(entity, joinContext(entity));
        } else {
            return entity;
        }
    }

    @Override
    public int previousIndex() {
        return this.target.previousIndex();
    }

    @Override
    public void remove() {
        log.trace("Mark list property {} of entity class {} dirty upon element removal",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        this.target.remove();
        this.markDirty();
    }

    @Override
    public void set(Object e) {
        log.trace("Mark list property {} of entity class {} dirty upon element set at current position",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        this.target.set(proxifier.unwrap(e));
        this.markDirty();
    }

}
