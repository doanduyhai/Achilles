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
public class ListIteratorWrapperBuilder extends AbstractWrapperBuilder<ListIteratorWrapperBuilder> {
    private ListIterator<Object> target;

    public static ListIteratorWrapperBuilder builder(PersistenceContext context, ListIterator<Object> target) {
        return new ListIteratorWrapperBuilder(context, target);
    }

    public ListIteratorWrapperBuilder(PersistenceContext context, ListIterator<Object> target) {
        super.context = context;
        this.target = target;
    }

    public ListIteratorWrapper build() {
        ListIteratorWrapper listIteratorWrapper = new ListIteratorWrapper(this.target);
        super.build(listIteratorWrapper);
        return listIteratorWrapper;
    }

}
