package info.archinnov.achilles.internal.metadata.parsing;

import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.type.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class TypeParser {

    private static final Logger log = LoggerFactory.getLogger(TypeParser.class);

    static <T> Class<T> inferValueClassForListOrSet(Type genericType, Class<?> entityClass) {
        log.debug("Infer parameterized value class for collection type {} of entity class {} ", genericType.toString(),
                entityClass.getCanonicalName());

        Class<T> valueClass;
        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (actualTypeArguments.length > 0) {
                Type type = actualTypeArguments[actualTypeArguments.length - 1];
                valueClass = getClassFromType(type);
            } else {
                throw new AchillesBeanMappingException("The type '" + genericType.getClass().getCanonicalName()
                        + "' of the entity '" + entityClass.getCanonicalName() + "' should be parameterized");
            }
        } else {
            throw new AchillesBeanMappingException("The type '" + genericType.getClass().getCanonicalName()
                    + "' of the entity '" + entityClass.getCanonicalName() + "' should be parameterized");
        }

        log.trace("Inferred value class : {}", valueClass.getCanonicalName());

        return valueClass;
    }

    static <K, V> Pair<Class<K>, Class<V>> determineMapGenericTypes(Field field) {
        log.trace("Determine generic types for field Map<K,V> {} of entity class {}", field.getName(), field
                .getDeclaringClass().getCanonicalName());

        Type genericType = field.getGenericType();
        ParameterizedType pt = (ParameterizedType) genericType;
        Type[] actualTypeArguments = pt.getActualTypeArguments();

        Class<K> keyClass = TypeParser.getClassFromType(actualTypeArguments[0]);
        Class<V> valueClass = TypeParser.getClassFromType(actualTypeArguments[1]);

        return Pair.create(keyClass, valueClass);
    }

    @SuppressWarnings("unchecked")
    static <T> Class<T> getClassFromType(Type type) {
        log.debug("Infer class from type {}", type);
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return (Class<T>) parameterizedType.getRawType();
        } else if (type instanceof Class) {
            return (Class<T>) type;
        } else {
            throw new IllegalArgumentException("Cannot determine java class of type '" + type + "'");
        }
    }

}
