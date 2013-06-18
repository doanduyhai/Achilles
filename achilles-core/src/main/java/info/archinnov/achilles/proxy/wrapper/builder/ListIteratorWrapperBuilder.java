package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.ListIteratorWrapper;
import java.util.ListIterator;

/**
 * ListIteratorWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListIteratorWrapperBuilder<V> extends AbstractWrapperBuilder<ListIteratorWrapperBuilder<V>, Void, V> {
    private ListIterator<V> target;

    public static <V> ListIteratorWrapperBuilder<V> builder(PersistenceContext context, ListIterator<V> target) {
        return new ListIteratorWrapperBuilder<V>(context, target);
    }

    public ListIteratorWrapperBuilder(PersistenceContext context, ListIterator<V> target) {
        super.context = context;
        this.target = target;
    }

    public ListIteratorWrapper<V> build() {
        ListIteratorWrapper<V> listIteratorWrapper = new ListIteratorWrapper<V>(this.target);
        super.build(listIteratorWrapper);
        return listIteratorWrapper;
    }

}
