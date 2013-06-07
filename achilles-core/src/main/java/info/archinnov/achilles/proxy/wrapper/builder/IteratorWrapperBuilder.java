package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.IteratorWrapper;
import java.util.Iterator;

/**
 * IteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class IteratorWrapperBuilder<V> extends AbstractWrapperBuilder<IteratorWrapperBuilder<V>, Void, V> {
    private Iterator<V> target;

    public static <V> IteratorWrapperBuilder<V> builder(AchillesPersistenceContext context, Iterator<V> target) {
        return new IteratorWrapperBuilder<V>(context, target);
    }

    public IteratorWrapperBuilder(AchillesPersistenceContext context, Iterator<V> target) {
        super.context = context;
        this.target = target;
    }

    public IteratorWrapper<V> build() {
        IteratorWrapper<V> iteratorWrapper = new IteratorWrapper<V>(this.target);
        super.build(iteratorWrapper);
        return iteratorWrapper;
    }
}
