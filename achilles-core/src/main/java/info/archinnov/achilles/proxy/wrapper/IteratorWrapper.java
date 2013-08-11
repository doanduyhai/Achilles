package info.archinnov.achilles.proxy.wrapper;

import info.archinnov.achilles.context.PersistenceContext;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IteratorWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorWrapper extends AbstractWrapper implements Iterator<Object> {
    private static final Logger log = LoggerFactory.getLogger(IteratorWrapper.class);

    protected Iterator<Object> target;

    public IteratorWrapper(Iterator<Object> target) {
        this.target = target;
    }

    @Override
    public boolean hasNext() {
        return this.target.hasNext();
    }

    @Override
    public Object next() {
        Object value = this.target.next();
        if (isJoin()) {
            log.trace("Build proxy for join entity for property {} of entity class {} upon next() call",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            PersistenceContext joinContext = context.createContextForJoin(propertyMeta.joinMeta(), value);
            return proxifier.buildProxy(value, joinContext);
        } else {
            return value;
        }
    }

    @Override
    public void remove() {
        log.trace("Mark property {} of entity class {} as dirty upon element removal",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        this.target.remove();
        this.markDirty();
    }
}
