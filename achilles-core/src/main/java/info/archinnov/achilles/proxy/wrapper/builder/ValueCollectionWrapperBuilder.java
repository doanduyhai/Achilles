package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.ValueCollectionWrapper;
import java.util.Collection;

/**
 * ValueCollectionWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ValueCollectionWrapperBuilder extends
        AbstractWrapperBuilder<ValueCollectionWrapperBuilder> {
    private Collection<Object> target;

    public ValueCollectionWrapperBuilder(PersistenceContext context, Collection<Object> target) {
        super.context = context;
        this.target = target;
    }

    public static ValueCollectionWrapperBuilder builder(PersistenceContext context,
            Collection<Object> target) {
        return new ValueCollectionWrapperBuilder(context, target);
    }

    public ValueCollectionWrapper build() {
        ValueCollectionWrapper valueCollectionWrapper = new ValueCollectionWrapper(this.target);
        super.build(valueCollectionWrapper);
        return valueCollectionWrapper;
    }

}
