package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
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

    public static <V> IteratorWrapperBuilder<V> builder(PersistenceContext context, Iterator<V> target) {
        return new IteratorWrapperBuilder<V>(context, target);
    }

    public IteratorWrapperBuilder(PersistenceContext context, Iterator<V> target) {
        super.context = context;
        this.target = target;
    }

    public IteratorWrapper<V> build() {
        IteratorWrapper<V> iteratorWrapper = new IteratorWrapper<V>(this.target);
        super.build(iteratorWrapper);
        return iteratorWrapper;
    }
}
