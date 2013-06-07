package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.ValueCollectionWrapper;
import java.util.Collection;

/**
 * ValueCollectionWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ValueCollectionWrapperBuilder<V> extends
        AbstractWrapperBuilder<ValueCollectionWrapperBuilder<V>, Void, V> {
    private Collection<V> target;

    public ValueCollectionWrapperBuilder(AchillesPersistenceContext context, Collection<V> target) {
        super.context = context;
        this.target = target;
    }

    public static <V> ValueCollectionWrapperBuilder<V> builder(AchillesPersistenceContext context,
            Collection<V> target) {
        return new ValueCollectionWrapperBuilder<V>(context, target);
    }

    public ValueCollectionWrapper<V> build() {
        ValueCollectionWrapper<V> valueCollectionWrapper = new ValueCollectionWrapper<V>(this.target);
        super.build(valueCollectionWrapper);
        return valueCollectionWrapper;
    }

}
