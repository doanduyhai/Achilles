package info.archinnov.achilles.proxy.wrapper;

import java.util.Collection;

/**
 * ValueCollectionWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ValueCollectionWrapper extends CollectionWrapper {

    public ValueCollectionWrapper(Collection<Object> target) {
        super(target);
    }

    @Override
    public boolean add(Object arg0) {
        throw new UnsupportedOperationException("This method is not supported for a key set");
    }

    @Override
    public boolean addAll(Collection<?> arg0) {
        throw new UnsupportedOperationException("This method is not supported for a key set");
    }
}
