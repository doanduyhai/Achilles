package fr.doan.achilles.metadata.builder;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.metadata.SimplePropertyMeta;
import fr.doan.achilles.validation.Validator;

public class SimplePropertyMetaBuilder<V extends Serializable> {

    private String propertyName;
    private Class<V> valueClass;
    private Method[] accessors;

    public static <V extends Serializable> SimplePropertyMetaBuilder<V> simplePropertyMetaBuilder(Class<V> valueClass) {
        return new SimplePropertyMetaBuilder<V>(valueClass);
    }

    public SimplePropertyMetaBuilder(Class<V> valueClass) {
        this.valueClass = valueClass;
    }

    public SimplePropertyMeta<V> build() {

        SimplePropertyMeta<V> meta = new SimplePropertyMeta<V>();
        this.build(meta);
        return meta;
    }

    protected void build(SimplePropertyMeta<V> meta) {

        Validator.validateNotBlank(propertyName, "propertyName");
        Validator.validateNotNull(valueClass, "valueClazz");
        Validator.validateNotNull(accessors, "accessors");
        Validator.validateSize(Arrays.asList(accessors), 2, "accessors");

        Serializer<?> valueSerializer = SerializerTypeInferer.getSerializer(valueClass);

        meta.setPropertyName(propertyName);
        meta.setValueClass(valueClass);
        meta.setValueSerializer(valueSerializer);
        meta.setGetter(accessors[0]);
        meta.setSetter(accessors[1]);
    }

    public SimplePropertyMetaBuilder<V> propertyName(String propertyName) {
        this.propertyName = propertyName;
        return this;
    }

    public SimplePropertyMetaBuilder<V> accessors(Method[] accessors) {
        this.accessors = accessors;
        return this;
    }
}
