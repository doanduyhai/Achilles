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
public class CollectionWrapperBuilder extends AbstractWrapperBuilder<CollectionWrapperBuilder> {
    private Collection<Object> target;

    public static CollectionWrapperBuilder builder(PersistenceContext context, Collection<Object> target) {
        return new CollectionWrapperBuilder(context, target);
    }

    public CollectionWrapperBuilder(PersistenceContext context, Collection<Object> target) {
        super.context = context;
        this.target = target;
    }

    public CollectionWrapper build() {
        CollectionWrapper collectionWrapper = new CollectionWrapper(this.target);
        super.build(collectionWrapper);
        return collectionWrapper;
    }

}
