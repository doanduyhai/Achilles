package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.CollectionWrapper;
import java.util.Collection;

/**
 * CollectionWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CollectionWrapperBuilder<V> extends AbstractWrapperBuilder<CollectionWrapperBuilder<V>, Void, V> {
    private Collection<V> target;

    public static <V> CollectionWrapperBuilder<V> builder(PersistenceContext context, Collection<V> target) {
        return new CollectionWrapperBuilder<V>(context, target);
    }

    public CollectionWrapperBuilder(PersistenceContext context, Collection<V> target) {
        super.context = context;
        this.target = target;
    }

    public CollectionWrapper<V> build() {
        CollectionWrapper<V> collectionWrapper = new CollectionWrapper<V>(this.target);
        super.build(collectionWrapper);
        return collectionWrapper;
    }

}
