package fr.doan.achilles.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import me.prettyprint.hector.api.Serializer;

@SuppressWarnings("rawtypes")
public class MapPropertyMeta<V extends Serializable> extends SimplePropertyMeta<V> {

    private Class<? extends Map> mapClass;
    private Class<? extends Serializable> keyClass;
    private Serializer<?> keySerializer;

    public Class getKeyClass() {
        return keyClass;
    }

    public Serializer<?> getKeySerializer() {
        return keySerializer;
    }

    public void setKeyClass(Class<? extends Serializable> keyClass) {
        this.keyClass = keyClass;
    }

    public void setKeySerializer(Serializer<?> keyClassSerializer) {
        this.keySerializer = keyClassSerializer;
    }

    @SuppressWarnings("unchecked")
    public <K extends Serializable> Map<K, V> newMapInstance() {
        Map<K, V> map;
        try {
            map = this.mapClass.newInstance();
        } catch (InstantiationException e) {
            map = new HashMap<K, V>();
        } catch (IllegalAccessException e) {
            map = new HashMap<K, V>();
        }
        return map;
    }

    public void setMapClass(Class<? extends Map> mapClass) {
        this.mapClass = mapClass;
    }

    @Override
    public PropertyType propertyType() {
        return PropertyType.MAP;
    }
}
