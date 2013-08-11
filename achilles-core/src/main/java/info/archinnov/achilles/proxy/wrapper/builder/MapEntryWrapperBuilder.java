package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.MapEntryWrapper;
import java.util.Map;

/**
 * MapEntryWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class MapEntryWrapperBuilder extends AbstractWrapperBuilder<MapEntryWrapperBuilder> {
    private final Map.Entry<Object, Object> target;

    public MapEntryWrapperBuilder(PersistenceContext context, Map.Entry<Object, Object> target) {
        super.context = context;
        this.target = target;
    }

    public static MapEntryWrapperBuilder builder(PersistenceContext context,
            Map.Entry<Object, Object> target) {
        return new MapEntryWrapperBuilder(context, target);
    }

    public MapEntryWrapper build() {
        MapEntryWrapper wrapper = new MapEntryWrapper(this.target);
        super.build(wrapper);
        return wrapper;
    }

}
