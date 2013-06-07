package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.EntrySetWrapper;
import java.util.Map.Entry;
import java.util.Set;

/**
 * EntrySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntrySetWrapperBuilder<K, V> extends AbstractWrapperBuilder<EntrySetWrapperBuilder<K, V>, K, V> {
    private Set<Entry<K, V>> target;

    public static <K, V> EntrySetWrapperBuilder<K, V> builder(AchillesPersistenceContext context,
            Set<Entry<K, V>> target) {
        return new EntrySetWrapperBuilder<K, V>(context, target);
    }

    public EntrySetWrapperBuilder(AchillesPersistenceContext context, Set<Entry<K, V>> target) {
        super.context = context;
        this.target = target;
    }

    public EntrySetWrapper<K, V> build() {
        EntrySetWrapper<K, V> entrySetWrapper = new EntrySetWrapper<K, V>(this.target);
        super.build(entrySetWrapper);
        return entrySetWrapper;
    }
}
