package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.ListWrapper;
import java.util.List;

/**
 * ListWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListWrapperBuilder extends AbstractWrapperBuilder<ListWrapperBuilder> {
    private List<Object> target;

    public static ListWrapperBuilder builder(PersistenceContext context, List<Object> target) {
        return new ListWrapperBuilder(context, target);
    }

    public ListWrapperBuilder(PersistenceContext context, List<Object> target) {
        super.context = context;
        this.target = target;
    }

    public ListWrapper build() {
        ListWrapper listWrapper = new ListWrapper(this.target);
        super.build(listWrapper);
        return listWrapper;
    }

}
