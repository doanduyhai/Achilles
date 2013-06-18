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
public class MapWrapperBuilder<K, V> extends AbstractWrapperBuilder<MapWrapperBuilder<K, V>, K, V> {
    private Map<K, V> target;

    public static <ID, K, V> MapWrapperBuilder<K, V> builder(PersistenceContext context, Map<K, V> target) {
        return new MapWrapperBuilder<K, V>(context, target);
    }

    public MapWrapperBuilder(PersistenceContext context, Map<K, V> target) {
        super.context = context;
        this.target = target;
    }

    public MapWrapper<K, V> build() {
        MapWrapper<K, V> mapWrapper = new MapWrapper<K, V>(this.target);
        super.build(mapWrapper);
        return mapWrapper;
    }
}
