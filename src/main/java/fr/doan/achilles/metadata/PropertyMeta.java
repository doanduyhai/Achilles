package fr.doan.achilles.metadata;

import java.io.Serializable;
import java.lang.reflect.Method;
import me.prettyprint.hector.api.Serializer;

public interface PropertyMeta<T extends Serializable> {

    public PropertyType propertyType();

    public String getPropertyName();

    public Class<T> getValueClass();

    public Serializer<?> getValueSerializer();

    public T get(Object object);

    public Method getGetter();

    public Method getSetter();
}
