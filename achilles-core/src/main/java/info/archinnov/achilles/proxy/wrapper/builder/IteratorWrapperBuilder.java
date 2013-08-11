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
public class IteratorWrapperBuilder extends AbstractWrapperBuilder<IteratorWrapperBuilder> {
    private Iterator<Object> target;

    public static IteratorWrapperBuilder builder(PersistenceContext context, Iterator<Object> target) {
        return new IteratorWrapperBuilder(context, target);
    }

    public IteratorWrapperBuilder(PersistenceContext context, Iterator<Object> target) {
        super.context = context;
        this.target = target;
    }

    public IteratorWrapper build() {
        IteratorWrapper iteratorWrapper = new IteratorWrapper(this.target);
        super.build(iteratorWrapper);
        return iteratorWrapper;
    }
}
