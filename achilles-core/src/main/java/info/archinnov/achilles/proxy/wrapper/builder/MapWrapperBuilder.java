package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.MapWrapper;
import java.util.Map;

/**
 * MapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class MapWrapperBuilder extends AbstractWrapperBuilder<MapWrapperBuilder> {
    private Map<Object, Object> target;

    public static MapWrapperBuilder builder(PersistenceContext context, Map<Object, Object> target) {
        return new MapWrapperBuilder(context, target);
    }

    public MapWrapperBuilder(PersistenceContext context, Map<Object, Object> target) {
        super.context = context;
        this.target = target;
    }

    public MapWrapper build() {
        MapWrapper mapWrapper = new MapWrapper(this.target);
        super.build(mapWrapper);
        return mapWrapper;
    }
}
