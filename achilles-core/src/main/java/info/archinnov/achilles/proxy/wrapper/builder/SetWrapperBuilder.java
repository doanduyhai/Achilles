package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.SetWrapper;
import java.util.Set;

/**
 * SetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SetWrapperBuilder extends AbstractWrapperBuilder<SetWrapperBuilder> {
    private Set<Object> target;

    public static SetWrapperBuilder builder(PersistenceContext context, Set<Object> target) {
        return new SetWrapperBuilder(context, target);
    }

    public SetWrapperBuilder(PersistenceContext context, Set<Object> target) {
        super.context = context;
        this.target = target;
    }

    public SetWrapper build() {
        SetWrapper setWrapper = new SetWrapper(this.target);
        super.build(setWrapper);
        return setWrapper;
    }

}
