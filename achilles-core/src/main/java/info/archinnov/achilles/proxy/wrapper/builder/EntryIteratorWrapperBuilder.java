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
public class EntryIteratorWrapperBuilder<K, V> extends
        AbstractWrapperBuilder<EntryIteratorWrapperBuilder<K, V>, K, V> {
    private Iterator<Entry<K, V>> target;

    public static <K, V> EntryIteratorWrapperBuilder<K, V> builder(PersistenceContext context,
            Iterator<Entry<K, V>> target) {
        return new EntryIteratorWrapperBuilder<K, V>(context, target);
    }

    public EntryIteratorWrapperBuilder(PersistenceContext context, Iterator<Entry<K, V>> target) {
        super.context = context;
        this.target = target;
    }

    public EntryIteratorWrapper<K, V> build() {
        EntryIteratorWrapper<K, V> iteratorWrapper = new EntryIteratorWrapper<K, V>(this.target);
        super.build(iteratorWrapper);
        return iteratorWrapper;
    }
}
