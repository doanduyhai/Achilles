package fr.doan.achilles.metadata.builder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.validation.Validator;

@SuppressWarnings("rawtypes")
public class MapPropertyMetaBuilder<V extends Serializable> extends SimplePropertyMetaBuilder<V> {

    private Class<? extends Map> mapClass;
    private Class<? extends Serializable> keyClass;

    public static <V extends Serializable> MapPropertyMetaBuilder<V> mapPropertyMetaBuilder(Class<V> valueClass) {
        return new MapPropertyMetaBuilder<V>(valueClass);
    }

    public MapPropertyMetaBuilder(Class<V> valueClass) {
        super(valueClass);
    }

    @Override
    public MapPropertyMeta<V> build() {

        Validator.validateNotNull(keyClass, "keyClass");
        Validator.validateNotNull(mapClass, "mapClass");

        MapPropertyMeta<V> meta = new MapPropertyMeta<V>();
        super.build(meta);

        Serializer<?> keySerializer = SerializerTypeInferer.getSerializer(keyClass);
        meta.setKeyClass(keyClass);
        meta.setKeySerializer(keySerializer);

        if (mapClass == Map.class) {
            meta.setMapClass(HashMap.class);
        } else {
            meta.setMapClass(mapClass);
        }
        return meta;
    }

    public MapPropertyMetaBuilder<V> mapClass(Class<? extends Map> mapClass) {
        this.mapClass = mapClass;
        return this;
    }

    public MapPropertyMetaBuilder<V> keyClass(Class<? extends Serializable> keyClass) {
        this.keyClass = keyClass;
        return this;
    }
}
