package info.archinnov.achilles.proxy.wrapper;

import java.util.Set;

/**
 * SetWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class SetWrapper extends CollectionWrapper implements Set<Object> {

    public SetWrapper(Set<Object> target) {
        super(target);
    }

    @Override
    public Set<Object> getTarget() {
        return ((Set<Object>) super.target);
    }
}
