package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.EntrySetWrapper;
import java.util.Map.Entry;
import java.util.Set;

/**
 * EntrySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntrySetWrapperBuilder extends AbstractWrapperBuilder<EntrySetWrapperBuilder> {
    private Set<Entry<Object, Object>> target;

    public static EntrySetWrapperBuilder builder(PersistenceContext context,
            Set<Entry<Object, Object>> target) {
        return new EntrySetWrapperBuilder(context, target);
    }

    public EntrySetWrapperBuilder(PersistenceContext context, Set<Entry<Object, Object>> target) {
        super.context = context;
        this.target = target;
    }

    public EntrySetWrapper build() {
        EntrySetWrapper entrySetWrapper = new EntrySetWrapper(this.target);
        super.build(entrySetWrapper);
        return entrySetWrapper;
    }
}
