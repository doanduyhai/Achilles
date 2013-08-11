package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.KeySetWrapper;
import java.util.Set;

/**
 * KeySetWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeySetWrapperBuilder extends AbstractWrapperBuilder<KeySetWrapperBuilder>
{
    private Set<Object> target;

    public KeySetWrapperBuilder(PersistenceContext context, Set<Object> target) {
        super.context = context;
        this.target = target;
    }

    public static KeySetWrapperBuilder builder(PersistenceContext context, Set<Object> target)
    {
        return new KeySetWrapperBuilder(context, target);
    }

    public KeySetWrapper build()
    {
        KeySetWrapper keySetWrapper = new KeySetWrapper(this.target);
        super.build(keySetWrapper);
        return keySetWrapper;
    }

}
