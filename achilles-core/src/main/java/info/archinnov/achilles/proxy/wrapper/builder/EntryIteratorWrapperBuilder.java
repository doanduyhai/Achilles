package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.EntryIteratorWrapper;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * EntryIteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntryIteratorWrapperBuilder extends AbstractWrapperBuilder<EntryIteratorWrapperBuilder> {
    private Iterator<Entry<Object, Object>> target;

    public static EntryIteratorWrapperBuilder builder(PersistenceContext context,
            Iterator<Entry<Object, Object>> target) {
        return new EntryIteratorWrapperBuilder(context, target);
    }

    public EntryIteratorWrapperBuilder(PersistenceContext context, Iterator<Entry<Object, Object>> target) {
        super.context = context;
        this.target = target;
    }

    public EntryIteratorWrapper build() {
        EntryIteratorWrapper iteratorWrapper = new EntryIteratorWrapper(this.target);
        super.build(iteratorWrapper);
        return iteratorWrapper;
    }
}
